from datetime import timedelta

import duckdb
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# DuckDB connection (in-memory, shared across helpers via module-level con)
# ---------------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_holdings(file_path: str) -> pd.DataFrame:
    """
    Read all quarterly 13F parquet files from file_path into a single DataFrame.
    Selects only the columns needed downstream.
    """
    df = con.execute(f"""
        SELECT
            CAST(CIK AS VARCHAR) AS CIK,
            CAST(FILINGMANAGER_NAME AS VARCHAR) AS FILINGMANAGER_NAME,
            CAST(PERIODOFREPORT AS DATE) AS PERIODOFREPORT,
            CAST(FILING_DATE AS DATE) AS FILING_DATE,
            CAST(TABLEVALUETOTAL AS DOUBLE) AS TABLEVALUETOTAL,
            CAST(VALUE AS BIGINT) AS VALUE,
            CAST(CUSIP AS VARCHAR) AS CUSIP,
            CAST(ticker AS VARCHAR) AS ticker,
            CAST(equity_portfolio_total AS BIGINT) AS equity_portfolio_total,
            CAST(equity_weight AS DOUBLE) AS equity_weight
        FROM read_parquet('{file_path}', hive_partitioning = false)
        ORDER BY CIK, PERIODOFREPORT
    """).df()
    return df


def load_prices(file_path: str) -> pd.DataFrame:
    """
    Read the consolidated stock-price parquet file.
    """
    df = con.execute(f"""
        SELECT
            CAST(date AS DATE)  AS date,
            CAST(ticker AS VARCHAR) AS ticker,
            CAST(adj_close AS DOUBLE) AS adj_close,
            CAST(open as DOUBLE) AS open,
            CAST((open * (adj_close / close)) AS DOUBLE) AS adj_open
        FROM read_parquet('{file_path}')
        ORDER BY ticker, date
    """).df()
    return df

# ---------------------------------------------------------------------------
# Filter dates based on user inputs of start and end date
# ---------------------------------------------------------------------------

def filter_dates(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Keep rows where PERIODOFREPORT is between start_date and end_date,
    PLUS the two most recent quarters available before start_date.
    """
    return con.execute(f"""
        WITH prior_periods AS (
            -- Get the 2 distinct latest report dates before the start_date
            SELECT DISTINCT PERIODOFREPORT
            FROM df
            WHERE PERIODOFREPORT < CAST('{start_date}' AS DATE)
            ORDER BY PERIODOFREPORT DESC
            LIMIT 2
        ),
        min_prior AS (
            -- Find the oldest of those 2 quarters
            SELECT MIN(PERIODOFREPORT) as start_cutoff FROM prior_periods
        )
        SELECT df.*
        FROM df
        WHERE PERIODOFREPORT >= COALESCE(
            (SELECT start_cutoff FROM min_prior), 
            CAST('{start_date}' AS DATE)
        )
          AND PERIODOFREPORT <= CAST('{end_date}' AS DATE)
        ORDER BY PERIODOFREPORT, ticker
    """).df()

# -----------------------------------------------------------------------------------------------
# Aggregation & ranking to get top 10 stocks per quarter by aggregated weight across institutions
# -----------------------------------------------------------------------------------------------

def aggregate_stock_weights(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sum equity_weight across institutions per (quarter, ticker).
    PERIODOFREPORT is the lag anchor (trade_date = PERIODOFREPORT + lag_days).
    """
    return con.execute("""
        SELECT
            PERIODOFREPORT,
            ticker,
            SUM(equity_weight) AS agg_weight
        FROM df
        GROUP BY PERIODOFREPORT, ticker
    """).df()


def rank_topN(df: pd.DataFrame, topN: int = 10) -> pd.DataFrame:
    """Rank stocks by agg_weight within each quarter; keep top N."""
    return con.execute("""
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY PERIODOFREPORT
                    ORDER BY agg_weight DESC, ticker ASC
                ) AS rank
            FROM df
        )
        WHERE rank <= ?
        ORDER BY PERIODOFREPORT, rank
    """, [topN]).df()

# -----------------------------------------------------------------------------------------------
# Get trade prices by applying filing lag logic and joining with price data
# Standard lag = 47 days. 45 days is the legal filing deadline, plus a buffer of 2 days for data availability and processing.
# -----------------------------------------------------------------------------------------------


def apply_filing_lag_and_get_trade_prices(df: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """
    For each (quarter, ticker):
      1. Compute candidate_date = PERIODOFREPORT + lag_days.
      2. Find the NEXT available trading day on-or-after candidate_date
         (snap FORWARD if it falls on a weekend / public holiday).
      3. Attach that day's OPEN price as entry_price (trade executed at open)
         and adj_close for audit/reference.
 
    Snapping forward: you can only act on 13F info after the
    filing lag has elapsed, so you would never trade on a date before
    the information is available.
 
    Returns df augmented with:
        candidate_date  -- PERIODOFREPORT + lag_days (before snapping): When the information is available.
        trade_date      -- first trading day >= candidate_date: The actual first day the market is open to let u execute the trade
        entry_price     -- adjusted open* on trade_date: The price you bought / sell at 
        adj_close       -- for evaluation of portfolio performance
    """
    result = con.execute(f"""
        WITH lagged AS (
            SELECT
                *,
                CAST(PERIODOFREPORT AS DATE) + INTERVAL '47 days' AS candidate_date
            FROM df
        )
        SELECT
            l.PERIODOFREPORT,
            l.ticker,
            l.agg_weight,
            l.rank,
            l.candidate_date,
            p.date      AS trade_date,
            p.adj_open AS entry_price,
            p.adj_close AS adj_close
        FROM lagged l
        JOIN prices p
          ON  p.ticker = l.ticker
          AND p.date  >= l.candidate_date
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY l.PERIODOFREPORT, l.ticker
            ORDER BY p.date ASC
        ) = 1
        ORDER BY l.PERIODOFREPORT, l.ticker
    """).df()
    return result

# -----------------------------------------------------------------------------------------------
# Extract price subset for only the tickers ever held in the backtest, to save memory in the backtest.
# -----------------------------------------------------------------------------------------------

def extract_price_subset(prices: pd.DataFrame, topN: pd.DataFrame) -> pd.DataFrame:
    """Filter prices to only the tickers ever held, saving memory in the backtest."""
    return con.execute("""
        SELECT p.*
        FROM prices p
        SEMI JOIN (SELECT DISTINCT ticker FROM topN) t
            ON p.ticker = t.ticker
        ORDER BY p.ticker, p.date
    """).df()


# ---------------------------------------------------------------------------
# Back-test  (equal-weight, quarterly rebalance, use smart rebalance to minimise unnecessary trading)
# ---------------------------------------------------------------------------
# need to put in transaction cost into this
# per traded dollar. 

def run_backtest(topN: pd.DataFrame,
                 prices: pd.DataFrame,
                 initial_capital: float,
                 cost_rate: float = 0.001,
                 start_date=None,
                 end_date=None) -> pd.DataFrame:
    """
    Equal-weight quarterly rebalance back-test.
 
    Mechanics
    ---------
    * trade_date = PERIODOFREPORT + lag_days (snapped forward to next trading day).
    * Each quarter's holding period: [trade_date[q], trade_date[q+1]).
    * On trade_date[q] SMART rebalancing is executed at the adjusted OPEN price:
        - exits   (dropped from top-N): sell all shares at open
        - entries (new to top-N):       buy target_allocation / open_price shares
        - stayers (in both quarters):    trade only the DELTA needed to restore
                                         equal weight (price drift during the quarter
                                         means their weights are no longer equal)
        - This minimises turnover vs a full sell-and-rebuy every quarter.
        - Final share counts are identical to a full rebuy, but fewer shares trade.
    * Daily portfolio value uses ADJ_CLOSE for mark-to-market:
        - portfolio_value[date] = sum(shares[ticker] * adj_close[ticker][date])
        - adj_close is dividend/split-adjusted so returns are total return, not price return.
    * portfolio_value carried into next quarter = adj_close mark-to-market on the
      last trading day of the current holding period (day before next trade_date).
 
    Why open for execution, adj_close for valuation?
    -------------------------------------------------
    Open price reflects the realistic fill price when you place a market order at
    the start of the day after you've decided to rebalance. adj_close is the standard
    for performance measurement because it accounts for dividends and splits, giving
    a true economic return. Mixing them correctly means: execution cost uses open,
    ongoing P&L uses adj_close.
 
    Returns
    -------
    DataFrame with columns:
        date             -- daily price observation dates
        quarter          -- PERIODOFREPORT the row belongs to
        trade_date       -- the actual date we execute the trade and the rebalance for that quarter (PERIODOFREPORT + lag, snapped forward)
        holding_period   -- period we hold the stocks for that quarter (e.g. "2020-02-15 to 2020-05-14")
        tickers          -- list of top-10 tickers held in that quarter
        portfolio_value  -- end-of-day mark-to-market value (shares * adj_close)
        daily_return     -- pct change vs previous trading day (NaN on first row)
        cum_return       -- cumulative return from inception
        quarter_return   -- total return for that quarter (repeated for every day in quarter)
    """
 
    # ---- normalise date types to Python date for consistent comparisons ----
    topN = topN.copy()
    topN["PERIODOFREPORT"] = pd.to_datetime(topN["PERIODOFREPORT"]).dt.date
    topN["trade_date"]     = pd.to_datetime(topN["trade_date"]).dt.date
 
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
 
    # ---- sort quarters ------------------------------------------------
    quarters = sorted(topN["PERIODOFREPORT"].unique())
    if len(quarters) < 2:
        raise ValueError(
            f"Need at least 2 quarters to run a backtest. "
            f"Found quarters: {quarters}"
        )
 
    # ---- trade_date per quarter --------------------------------------
    trade_date_map: dict = (
        topN.groupby("PERIODOFREPORT")["trade_date"]
             .first()
             .to_dict()
    )

    # ---- SNAP FIRST TRADE DATE to start_date if needed ---------------
    # If start_date falls between first and second quarter's trade_date,
    # override the first quarter's trade_date to start_date so the
    # holding period begins from the user's requested start date.
    if start_date is not None:
        start_dt = pd.to_datetime(start_date).date()
        available_dates = sorted(prices["date"].unique())

        # Drop any quarters whose trade_date is already before start_date
        # (these were pulled in by filter_dates but are not needed)
        quarters_before_start = [
            q for q in quarters
            if trade_date_map[q] < start_dt
        ]
        if quarters_before_start:
            last_before = max(quarters_before_start)  # the quarter straddling start_date
            # Drop all quarters strictly before last_before
            quarters = [q for q in quarters if q >= last_before]
            topN = topN[topN["PERIODOFREPORT"] >= last_before].copy()

            # Snap start_date forward to next available trading day
            snapped = next((d for d in available_dates if d >= start_dt), None)
            if snapped is None:
                raise ValueError(f"No trading days found on or after start_date {start_date}")

            # Override that quarter's trade_date to snapped start_date
            trade_date_map[last_before] = snapped
            topN.loc[topN["PERIODOFREPORT"] == last_before, "trade_date"] = snapped

 
    # ---- tickers per quarter -----------------------------------------
    tickers_map: dict = (
        topN.groupby("PERIODOFREPORT")["ticker"]
             .apply(list)
             .to_dict()
    )

    # ---- trade_date label per quarter (for output column) ------------
    # Same values as trade_date_map but kept separate for clarity
    trade_date_label_map: dict = trade_date_map.copy()

    # ---- EXTEND OR TRUNCATE TO end_date using last quarter's holdings ----
    if end_date is not None:
        end_date = pd.to_datetime(end_date).date()
        last_quarter = quarters[-1]
        last_trade_date = trade_date_map[last_quarter]

        if end_date > last_trade_date:
            # Extend: add phantom quarter to push the last holding period to end_date
            phantom_quarter = end_date + timedelta(days=1)

            trade_date_map[phantom_quarter]       = phantom_quarter
            tickers_map[phantom_quarter]          = tickers_map[last_quarter]
            trade_date_label_map[phantom_quarter] = phantom_quarter

            quarters = quarters + [phantom_quarter]

        elif end_date < last_trade_date:
            # Truncate: drop all quarters whose trade_date is after end_date,
            # then add a phantom to act as the period_end boundary
            quarters = [q for q in quarters if trade_date_map[q] <= end_date]
            last_quarter = quarters[-1]

            phantom_quarter = end_date + timedelta(days=1)

            trade_date_map[phantom_quarter]       = phantom_quarter
            tickers_map[phantom_quarter]          = tickers_map[last_quarter]
            trade_date_label_map[phantom_quarter] = phantom_quarter

            quarters = quarters + [phantom_quarter]
 
    # ---- holding period per quarter ----------------------------------
    # holding_period = trade_date[q] to last trading day before trade_date[q+1]
    # Built after adj_close_wide so we can look up the last actual trading day.
    # Populated in the loop below once adj_close_wide is available.
    holding_period_map: dict = {}

    # ---- wide adj_close table for daily valuation --------------------
    # rows = trading dates, columns = tickers, values = adj_close
    adj_close_wide = (
        prices
        .pivot_table(index="date", columns="ticker", values="adj_close")
        .sort_index()
    )
    adj_close_wide = adj_close_wide.ffill()   # forward-fill any gaps
 
    # ---- wide adj_open table for rebalance execution ---------------
    # Only needed on trade_dates; we look up per-ticker adj_open on demand
    adj_open_wide = (
        prices
        .pivot_table(index="date", columns="ticker", values="adj_open")
        .sort_index()
    )
 
    # ---- run quarter-by-quarter ---------------------------------------
    portfolio_value = initial_capital
    positions: dict[str, float] = {}   # ticker -> shares held (fixed within quarter)
    history: list[dict] = []
 
    for i in range(len(quarters) - 1):
        q_now   = quarters[i]
        q_next  = quarters[i + 1]
 
        period_start = trade_date_map[q_now]    # inclusive: rebalance + first valuation day
        period_end   = trade_date_map[q_next]   # exclusive: next rebalance date
 
        stocks_now = topN[topN["PERIODOFREPORT"] == q_now].set_index("ticker")
        q_tickers  = tickers_map[q_now]
 
        # holding period: trade_date[q] to last trading day before trade_date[q+1]
        holding_period_map[q_now] = f"{period_start} to {period_end}"
 
        # ── Get adj_open prices for this rebalance date ───────────────────
        if period_start not in adj_open_wide.index:
            raise ValueError(f"No adjusted open data for trade date {period_start}")
        adj_open_row = adj_open_wide.loc[period_start]
 
        def get_price(ticker: str) -> float:
            """Return adjusted open price for ticker on rebalance date."""
            val = adj_open_row.get(ticker, float("nan"))
            return float(val) if pd.notna(val) and val > 0 else float("nan")
 
        new_tickers  = set(stocks_now.index)
        prev_tickers = set(positions.keys())
 
        exits   = prev_tickers - new_tickers          # sell completely
        entries = new_tickers  - prev_tickers         # buy fresh
        stayers = prev_tickers & new_tickers          # adjust weight only
 
        # ── Step 1: mark total portfolio value to market at today's adj_open ─
        # We need the total $ value to compute equal-weight target allocations.
        if positions:
            portfolio_value = sum(
                shares * get_price(tkr)
                for tkr, shares in positions.items()
                if not np.isnan(get_price(tkr))
            )
 
        # ── Step 2: rebalance with transaction costs ─────────────

        # Step 2a — compute CURRENT values at adj_open
        current_values = {}
        for tkr, shares in positions.items():
            px = get_price(tkr)
            if not np.isnan(px):
                current_values[tkr] = shares * px

        if current_values:
            portfolio_value = sum(current_values.values())

        n_stocks = len(stocks_now)
        if n_stocks == 0:
            continue

        # Step 2b — first-pass target allocation (before cost)
        pre_cost_portfolio_value = float(portfolio_value)
        target_allocation = portfolio_value / n_stocks

        # Step 2c — compute turnover
        turnover = 0.0

        all_tickers = set(current_values.keys()) | set(stocks_now.index)

        for tkr in all_tickers:
            current_val = current_values.get(tkr, 0.0)
            target_val  = target_allocation if tkr in stocks_now.index else 0.0
            turnover += abs(target_val - current_val)

        # Step 2d — compute transaction cost
        transaction_cost = cost_rate * turnover

        # Step 2e — deduct cost
        portfolio_value -= transaction_cost

        if portfolio_value <= 0:
            raise ValueError("Portfolio value wiped out by transaction costs.")

        # Step 2f — recompute target allocation AFTER cost
        target_allocation = portfolio_value / n_stocks

        # Step 2g — set final positions
        new_positions: dict[str, float] = {}

        for ticker, srow in stocks_now.iterrows():
            adj_open_price = get_price(ticker)
            if np.isnan(adj_open_price) or adj_open_price <= 0:
                continue

            new_positions[ticker] = target_allocation / adj_open_price

        positions = new_positions

        quarter_start_value = float(portfolio_value)
        quarter_start_value_gross = float(pre_cost_portfolio_value)
 
        # ── Step 3: daily mark-to-market using adj_close ───────────────
        mask = (adj_close_wide.index >= period_start) & (adj_close_wide.index < period_end)
        period_adj = adj_close_wide.loc[mask]
 
        held_tickers = [t for t in positions if t in period_adj.columns]
        shares_vec   = np.array([positions[t] for t in held_tickers])
 
        # Fast vectorised dot product: (n_days x n_tickers) @ (n_tickers,) = (n_days,)
        daily_values = period_adj[held_tickers].values @ shares_vec
 
        for date, val in zip(period_adj.index, daily_values):
            history.append({
                "date":            date,
                "quarter":         q_now,
                "trade_date":      trade_date_label_map[q_now],
                "holding_period":  holding_period_map[q_now],
                "tickers":         q_tickers,
                "portfolio_value": float(val),
                "_q_start_val":    quarter_start_value,
                "_q_start_val_gross": quarter_start_value_gross,

                "_turnover":        turnover,
                "_transaction_cost": transaction_cost,
            })
    # ---- assemble output ----------------------------------------------
    result = pd.DataFrame(history)
    result = result.sort_values("date").reset_index(drop=True)
 
    # daily_return: pct change across consecutive trading days (crosses quarter boundaries)
    result["daily_return"] = result["portfolio_value"].pct_change()
    result.loc[result.index[0], "daily_return"] = 0.0

    # cum_return: total return from inception
    first_value = result["portfolio_value"].iloc[0]
    result["cum_return"] = (result["portfolio_value"] / first_value) - 1
 
    # quarter_return: (last adj_close value of quarter / first adj_close value) - 1
    q_end = result.groupby("quarter")["portfolio_value"].last().rename("_q_end_val")
    result = result.join(q_end, on="quarter")
    result["quarter_return"] = (result["_q_end_val"] / result["_q_start_val"]) - 1

    # ---- turnover per quarter --------------------------------
    q_turnover = result.groupby("quarter")["_turnover"].first()
    result = result.join(q_turnover.rename("turnover"), on="quarter")

    # ---- transaction cost per quarter -------------------------
    q_cost = result.groupby("quarter")["_transaction_cost"].first()
    result = result.join(q_cost.rename("transaction_cost"), on="quarter")

    # ---- cost drag (% of capital lost to cost) ----------------
    # Computed before dropping _q_start_val to avoid KeyError
    result["cost_drag"] = result["transaction_cost"] / result["_q_start_val_gross"]

    # clean up temp cols
    result = result.drop(columns=["_q_start_val", "_q_start_val_gross", "_q_end_val", "_turnover", "_transaction_cost"])
 
    # reorder columns cleanly
    result = result[[
            "date", "quarter", "trade_date", "holding_period", "tickers",
            "portfolio_value", "daily_return", "cum_return", "quarter_return",
            "turnover", "transaction_cost", "cost_drag"
        ]]
 
    return result

def get_spy_df(spy_df: pd.DataFrame, start_date: str, end_date: str, initial_capital: float) -> pd.DataFrame:

    # 1. Prep SPY side
    spy = spy_df[["date", "adj_close"]].copy()
    spy["date"] = pd.to_datetime(spy["date"])
    spy = spy.sort_values("date").reset_index(drop=True)

    # 2. Filter SPY to strategy date range
    spy = spy[(spy["date"] >= pd.to_datetime(start_date)) & (spy["date"] <= pd.to_datetime(end_date))]

    # 3. Compute SPY daily return and cum return, anchored to same start as strategy
    spy["spy_daily_return"] = spy["adj_close"].pct_change()
    spy.loc[spy.index[0], "spy_daily_return"] = 0.0  # anchor first day to 0
    spy["spy_cum_return"] = (1 + spy["spy_daily_return"]).cumprod() - 1
    spy["spy_portfolio_value"] = initial_capital * (1 + spy["spy_cum_return"])

    return spy[["date", "spy_daily_return", "spy_cum_return", "spy_portfolio_value"]]