from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
import numpy as np
import pandas as pd
import re
from config import DEBUG, SCREENED_DIR, PRICES_FILE_FULL, BEST_INSTITUTION_RANKING_DIR

# =========================================================
# CONFIG
# =========================================================
FILINGS_DIR = SCREENED_DIR
PRICE_PATH  = PRICES_FILE_FULL
OUTPUT_DIR  = BEST_INSTITUTION_RANKING_DIR

START_YEAR = 2013
MIN_VALID_PERIODS_PER_FUND = 4

# Copycat rule:
# filing becomes public -> portfolio becomes actionable on the first trading day
# strictly after filing date.
# Returns are measured using corporate-action-adjusted open-to-open prices:
#   adj_open = open * (adj_close / close)
# Note: raw_open is only used as a data-availability check, not as the entry
# price in the return calculation.
ENTRY_LAG_TRADING_DAYS = 1
USE_US_ONLY = True

# Keep this off for now. Coverage is still measured and written out.
APPLY_COVERAGE_FILTER = False
COVERAGE_THRESHOLD = 0.80

# If True, unmatched weight earns 0% return (recommended baseline).
# If False, matched names are renormalized to 100%. 
# Select False as recommended by Prof 
MISSING_WEIGHT_AS_CASH = False

RISK_FREE_RATE = 0.0

# Carry the most recent disclosed portfolio forward to this backtest cut-off
# date when there is no subsequent filing-based rebalance yet.
BACKTEST_END_DATE = pd.Timestamp("2026-03-31")

# =========================================================
# HELPERS
# =========================================================


def normalize_ticker_for_prices(x: object) -> str | None:
    if pd.isna(x):
        return None

    t = str(x).strip().upper()
    if not t:
        return None

    t = t.replace(" ", "")
    t = t.replace("/", "-")
    t = t.replace(".", "-")
    t = t.replace("_", "-")
    t = re.sub(r"-{2,}", "-", t)
    t = t.strip("-")
    return t if t else None


def parse_boolish(x: object) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip().upper()
    return s in {"1", "TRUE", "T", "Y", "YES"}


def first_existing_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    if required:
        raise KeyError(f"None of these columns were found: {candidates}")
    return None


def nth_trading_date_after(trading_dates: pd.DatetimeIndex, dt: pd.Timestamp, n: int = 1) -> pd.Timestamp | pd.NaT:
    """
    n=1 => first trading date strictly AFTER dt.
    """
    pos = trading_dates.searchsorted(dt, side="right") + (n - 1)
    if pos >= len(trading_dates):
        return pd.NaT
    return trading_dates[pos]


def price_on_or_after(prices_one: pd.DataFrame, dt: pd.Timestamp, price_col: str) -> float | np.nan:
    idx = prices_one["date"].searchsorted(dt, side="left")
    if idx >= len(prices_one):
        return np.nan
    return float(prices_one.iloc[idx][price_col])


# =========================================================
# 13F DATA
# =========================================================


def read_all_13f_data(filings_dir: Path) -> pd.DataFrame:
    files = sorted(filings_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {filings_dir}")
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)


def load_13f_data(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cik_col = first_existing_column(df_raw, ["CIK"])
    period_col = first_existing_column(df_raw, ["PERIODOFREPORT"])
    filing_col = first_existing_column(df_raw, ["FILING_DATE"])
    sub_type_col = first_existing_column(df_raw, ["SUBMISSIONTYPE"], required=False)
    manager_col = first_existing_column(df_raw, ["FILINGMANAGER_NAME"], required=False)
    issuer_col = first_existing_column(df_raw, ["NAMEOFISSUER"], required=False)
    cusip_col = first_existing_column(df_raw, ["CUSIP"], required=False)
    value_col = first_existing_column(df_raw, ["VALUE"])
    security_type_col = first_existing_column(df_raw, ["security_type", "SECURITY_TYPE", "securityType"])
    exch_code_col = first_existing_column(df_raw, ["exchCode", "EXCHCODE", "EXCH code", "EXCH CODE"])
    sshprnamt_type_col = first_existing_column(df_raw, ["SSHPRNAMTTYPE"], required=False)
    putcall_col = first_existing_column(df_raw, ["PUTCALL"], required=False)
    ticker_col = first_existing_column(df_raw, ["TICKER", "ticker", "MAPPED_TICKER", "mapped_ticker"], required=False)

    if ticker_col is None:
        raise KeyError("Could not find a ticker column in the 13F parquet files.")

    out = df_raw.copy()
    out["CIK"] = out[cik_col].astype(str).str.strip()
    out["PERIODOFREPORT"] = pd.to_datetime(out[period_col], errors="coerce").dt.normalize()
    out["FILING_DATE"] = pd.to_datetime(out[filing_col], errors="coerce").dt.normalize()
    out["ticker_raw"] = out[ticker_col]
    out["ticker_bt"] = out["ticker_raw"].map(normalize_ticker_for_prices)
    out["VALUE"] = pd.to_numeric(out[value_col], errors="coerce")
    out["FILINGMANAGER_NAME"] = out[manager_col] if manager_col else out["CIK"]
    out["NAMEOFISSUER"] = out[issuer_col] if issuer_col else pd.NA
    out["CUSIP"] = out[cusip_col] if cusip_col else pd.NA
    out["security_type"] = out[security_type_col]
    out["exchCode"] = out[exch_code_col]

    if sub_type_col:
        print("SUBMISSIONTYPE values:")
        print(df_raw[sub_type_col].astype(str).str.upper().value_counts(dropna=False).head(20))

    if sub_type_col:
        out = out[out[sub_type_col].astype(str).str.upper().isin(["13F-HR", "13F-HR/A"])].copy()

    out = out[out["security_type"].astype(str).str.strip().str.upper().eq("COMMON STOCK")].copy()

    if sshprnamt_type_col:
        keep_sh = out[sshprnamt_type_col].astype(str).str.upper().str.strip().eq("SH")
        if keep_sh.sum() > 0:
            out = out[keep_sh].copy()

    if putcall_col:
        putcall_series = out[putcall_col].astype(str).str.upper().str.strip()
        out = out[(out[putcall_col].isna()) | (putcall_series.eq(""))].copy()

    # Discretion filter removed: a copycat investor replicates the full
    # disclosed portfolio regardless of how discretion is categorised
    # (SOLE / SHARED / DEFINED).  Keeping all types gives the true total
    # position per stock.

    out["is_us_exchange"] = out["exchCode"].astype(str).str.strip().str.upper().eq("US")

    # Drop rows missing essential identifiers or with no economic value.
    # NOTE: ticker_bt is deliberately NOT required here.  Holdings without
    # a mapped ticker still contribute to the portfolio-value denominator
    # so that weights reflect the full disclosed portfolio.  The backtest
    # return calculation will simply not be able to price those names and
    # the single renormalisation (MISSING_WEIGHT_AS_CASH=False) handles
    # the gap at that stage.
    out = out.dropna(subset=["CIK", "PERIODOFREPORT", "FILING_DATE", "VALUE"]).copy()
    out = out[out["VALUE"] > 0].copy()
    out = out[out["PERIODOFREPORT"].dt.year >= START_YEAR].copy()

    diagnostics = pd.DataFrame([
        {
            "rows_after_standard_filters": len(out),
            "unique_tickers_after_standard_filters": out["ticker_bt"].nunique(),
            "total_value_after_standard_filters": float(out["VALUE"].sum()),
            "rows_us_exchange": int(out["is_us_exchange"].sum()),
            "unique_us_tickers": out.loc[out["is_us_exchange"], "ticker_bt"].nunique(),
            "total_us_value": float(out.loc[out["is_us_exchange"], "VALUE"].sum()),
            "us_row_share": float(out["is_us_exchange"].mean()) if len(out) else np.nan,
            "us_value_share": float(out.loc[out["is_us_exchange"], "VALUE"].sum() / out["VALUE"].sum()) if out["VALUE"].sum() > 0 else np.nan,
        }
    ])

    if USE_US_ONLY:
        out = out[out["is_us_exchange"]].copy()

    return out, diagnostics


def build_quarter_holdings(df_13f: pd.DataFrame) -> pd.DataFrame:
    """
    Build fund-quarter holdings and weights using US-only common stocks.
    Weight = VALUE / total VALUE of ALL disclosed common-stock holdings in
    that fund-quarter (including those without a mapped ticker).

    This ensures the denominator reflects the full disclosed portfolio so
    that the priced-weight renormalisation at return time is the single
    point where unmapped / unpriceable holdings are handled.
    """
    quarter_keys = ["CIK", "PERIODOFREPORT", "FILING_DATE"]

    # Confidential holdings are kept — they still contribute to the
    # portfolio denominator.  If they have a valid ticker they will be
    # priced normally; if not, the renormalisation handles them.

    # Aggregate VALUE across all holdings (including NaN tickers) for the
    # denominator.
    quarter_totals = (
        df_13f.groupby(quarter_keys, as_index=False)["VALUE"]
        .sum()
        .rename(columns={"VALUE": "us_common_stock_value_total"})
    )

    # Now group only rows that have a usable ticker for the backtest.
    has_ticker = df_13f.dropna(subset=["ticker_bt"]).copy()
    holdings = (
        has_ticker.groupby(quarter_keys + ["FILINGMANAGER_NAME", "ticker_bt"], as_index=False)["VALUE"]
        .sum()
    )

    holdings = holdings.merge(quarter_totals, on=quarter_keys, how="left")
    holdings["weight"] = holdings["VALUE"] / holdings["us_common_stock_value_total"]
    holdings = holdings.sort_values(["CIK", "FILING_DATE", "ticker_bt"]).reset_index(drop=True)
    return holdings


# =========================================================
# PRICE DATA
# =========================================================


def load_price_data(price_path: Path) -> tuple[dict[str, pd.DataFrame], pd.DatetimeIndex, str, str]:
    """
    Load Yahoo open/close/adj_close data and derive adjusted-open:
        adj_open = open * (adj_close / close)

    We keep both columns:
    - raw_open: used only as an execution-availability check on the rebalance date
    - adj_open: used for return measurement (entry and exit) in the backtest

    Because this backtest is weight-based rather than share-ledger-based, you do
    NOT need daily path data to measure period returns. The backtest measures
    period returns from adjusted-open to adjusted-open over each holding window.
    """
    px = pd.read_parquet(price_path)

    date_col = first_existing_column(px, ["date", "DATE"])
    ticker_col = first_existing_column(px, ["ticker", "TICKER"])
    open_col = first_existing_column(px, ["open", "OPEN"])
    close_col = first_existing_column(px, ["close", "CLOSE"])
    adj_close_col = first_existing_column(px, ["adj_close", "ADJ_CLOSE"])

    out = px.copy()
    out["date"] = pd.to_datetime(out[date_col], errors="coerce").dt.normalize()
    out["ticker"] = out[ticker_col].astype(str).str.upper().map(normalize_ticker_for_prices)
    out["raw_open"] = pd.to_numeric(out[open_col], errors="coerce")
    out["close"] = pd.to_numeric(out[close_col], errors="coerce")
    out["adj_close"] = pd.to_numeric(out[adj_close_col], errors="coerce")

    out = out.dropna(subset=["date", "ticker", "raw_open", "close", "adj_close"]).copy()
    out = out[(out["raw_open"] > 0) & (out["close"] > 0) & (out["adj_close"] > 0)].copy()

    out["adj_factor"] = out["adj_close"] / out["close"]
    out["adj_open"] = out["raw_open"] * out["adj_factor"]

    out = out.dropna(subset=["adj_open"]).copy()
    out = out[out["adj_open"] > 0].copy()
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    out = out[["date", "ticker", "raw_open", "adj_open"]].copy()

    trading_dates = pd.DatetimeIndex(out["date"].drop_duplicates().sort_values())
    price_map = {
        t: g[["date", "raw_open", "adj_open"]].sort_values("date").reset_index(drop=True)
        for t, g in out.groupby("ticker", sort=False)
    }
    return price_map, trading_dates, "raw_open", "adj_open"


# =========================================================
# BACKTEST
# =========================================================

@dataclass
class PeriodResult:
    CIK: str
    FILINGMANAGER_NAME: str
    holding_period_start: pd.Timestamp
    holding_period_end: pd.Timestamp
    holding_period_end_source: str
    is_terminal_period: bool
    source_periodofreport: pd.Timestamp
    source_filing_date: pd.Timestamp
    priced_weight: float
    n_candidate_names: int
    n_priced_names: int
    period_return: float


def compute_priced_weight(
    portfolio: pd.DataFrame,
    price_map: dict[str, pd.DataFrame],
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    trade_entry_col: str,
    return_price_col: str,
) -> tuple[float, int, int]:
    """
    Coverage metric:
    priced_weight = total portfolio weight with a valid raw_open observation on
    the entry date and valid adjusted-open prices on both entry and exit.

    raw_open is used only to confirm the name was tradeable on the rebalance
    date; returns themselves are measured using adjusted-open prices.
    """
    if portfolio is None or portfolio.empty:
        return 0.0, 0, 0

    priced_weight = 0.0
    priced_count = 0
    total_count = len(portfolio)

    for row in portfolio.itertuples(index=False):
        ticker = row.ticker_bt
        weight = float(row.weight)
        px = price_map.get(ticker)
        if px is None or px.empty:
            continue

        entry_trade = price_on_or_after(px, period_start, trade_entry_col)
        entry_ret = price_on_or_after(px, period_start, return_price_col)
        exit_ret = price_on_or_after(px, period_end, return_price_col)
        if (
            pd.isna(entry_trade)
            or entry_trade <= 0
            or pd.isna(entry_ret)
            or entry_ret <= 0
            or pd.isna(exit_ret)
            or exit_ret <= 0
        ):
            continue

        priced_weight += weight
        priced_count += 1

    return priced_weight, priced_count, total_count



def period_return_for_portfolio(
    portfolio: pd.DataFrame,
    price_map: dict[str, pd.DataFrame],
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    trade_entry_col: str,
    return_price_col: str,
) -> tuple[float, float, int, int]:
    """
    Portfolio return with:
    - raw_open used only as a tradeability / data-availability check on the
      rebalance date
    - performance measured using adjusted-open to adjusted-open returns

    The economic return calculation enters at adjusted open and exits at
    adjusted open. This is a valuation-based backtest, not a literal raw-open
    execution backtest.

    Since this is a weight-based backtest, period return is computed from the
    adjusted valuation series. Daily data is not required unless you want a
    daily portfolio NAV path, share ledger, or more detailed transaction-cost
    modelling.

    If MISSING_WEIGHT_AS_CASH is True:
        unmatched weight is treated as zero-return residual.

    If False:
        matched names are renormalized to 100%.
    """
    if portfolio is None or portfolio.empty:
        return np.nan, 0.0, 0, 0

    weighted_ret = 0.0
    priced_weight = 0.0
    priced_count = 0
    total_count = len(portfolio)

    for row in portfolio.itertuples(index=False):
        ticker = row.ticker_bt
        weight = float(row.weight)
        px = price_map.get(ticker)
        if px is None or px.empty:
            continue

        entry_trade = price_on_or_after(px, period_start, trade_entry_col)
        entry_ret = price_on_or_after(px, period_start, return_price_col)
        exit_ret = price_on_or_after(px, period_end, return_price_col)
        if (
            pd.isna(entry_trade)
            or entry_trade <= 0
            or pd.isna(entry_ret)
            or entry_ret <= 0
            or pd.isna(exit_ret)
            or exit_ret <= 0
        ):
            continue

        r = (exit_ret / entry_ret) - 1.0
        weighted_ret += weight * r
        priced_weight += weight
        priced_count += 1

    if priced_count == 0:
        return np.nan, 0.0, 0, total_count

    if not MISSING_WEIGHT_AS_CASH:
        weighted_ret = weighted_ret / priced_weight

    return weighted_ret, priced_weight, priced_count, total_count



def max_drawdown_from_returns(returns: pd.Series) -> float:
    if returns.empty:
        return np.nan
    wealth = (1.0 + returns).cumprod()
    running_peak = wealth.cummax()
    drawdown = wealth / running_peak - 1.0
    return float(drawdown.min())



def run_backtest(
    holdings: pd.DataFrame,
    price_map: dict[str, pd.DataFrame],
    trading_dates: pd.DatetimeIndex,
    trade_entry_col: str,
    return_price_col: str,
    apply_coverage_filter: bool = False,
    coverage_threshold: float = 0.80,
    backtest_end_date: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each institution / CIK:
    - filing becomes public
    - rebalance date = n-th trading day strictly after that filing date
    - candidate portfolio uses VALUE weights over US common stocks
    - enter after that manager's filing becomes public
    - hold until that SAME manager's next filing-based rebalance date
    - if there is no next filing yet, carry the last disclosed portfolio forward
      to the configured backtest cut-off date (rather than dropping it)
    - raw_open is used only to verify that an entry-day market open exists
    - period return is measured using adjusted-open to adjusted-open valuation
    """
    if len(trading_dates) == 0:
        raise ValueError("No trading dates found in price data.")

    quarter_meta = (
        holdings.groupby(["CIK", "PERIODOFREPORT", "FILING_DATE", "FILINGMANAGER_NAME"], as_index=False)
        .agg(
            us_common_stock_value_total=("us_common_stock_value_total", "first"),
            n_names=("ticker_bt", "nunique"),
        )
        .sort_values(["CIK", "FILING_DATE", "PERIODOFREPORT"])
        .reset_index(drop=True)
    )

    quarter_meta["rebalance_date"] = quarter_meta["FILING_DATE"].map(
        lambda d: nth_trading_date_after(trading_dates, d, ENTRY_LAG_TRADING_DAYS)
    )
    quarter_meta = quarter_meta.dropna(subset=["rebalance_date"]).copy()

    if backtest_end_date is None:
        effective_backtest_end = trading_dates.max()
    else:
        effective_backtest_end = pd.Timestamp(backtest_end_date).normalize()
        eligible_end_dates = trading_dates[trading_dates <= effective_backtest_end]
        if len(eligible_end_dates) == 0:
            raise ValueError("Configured backtest_end_date is earlier than the first available trading date.")
        effective_backtest_end = eligible_end_dates.max()

    # Next rebalance is computed strictly within the same CIK/manager history.
    quarter_meta["next_rebalance_date"] = quarter_meta.groupby("CIK", sort=False)["rebalance_date"].shift(-1)
    quarter_meta["is_terminal_period"] = quarter_meta["next_rebalance_date"].isna()
    quarter_meta["holding_period_end"] = quarter_meta["next_rebalance_date"].fillna(effective_backtest_end)
    quarter_meta["holding_period_end_source"] = np.where(
        quarter_meta["is_terminal_period"],
        "backtest_end_date",
        "next_rebalance_date",
    )

    quarter_meta = quarter_meta[quarter_meta["holding_period_end"] > quarter_meta["rebalance_date"]].copy()
    quarter_meta = quarter_meta.sort_values(["CIK", "rebalance_date", "PERIODOFREPORT"]).reset_index(drop=True)

    holdings_lookup = {
        (row_cik, row_por, row_fd): grp[["ticker_bt", "weight"]].copy().reset_index(drop=True)
        for (row_cik, row_por, row_fd), grp in holdings.groupby(["CIK", "PERIODOFREPORT", "FILING_DATE"], sort=False)
    }

    results: list[PeriodResult] = []

    for cik, fund_quarters in quarter_meta.groupby("CIK", sort=False):
        fund_quarters = fund_quarters.sort_values(["rebalance_date", "PERIODOFREPORT"]).reset_index(drop=True)
        fund_name = fund_quarters["FILINGMANAGER_NAME"].iloc[0]

        for q in fund_quarters.itertuples(index=False):
            key = (q.CIK, q.PERIODOFREPORT, q.FILING_DATE)
            candidate = holdings_lookup.get(key)
            if candidate is None or candidate.empty:
                candidate = pd.DataFrame(columns=["ticker_bt", "weight"])

            priced_weight, priced_count, total_count = compute_priced_weight(
                portfolio=candidate,
                price_map=price_map,
                period_start=q.rebalance_date,
                period_end=q.holding_period_end,
                trade_entry_col=trade_entry_col,
                return_price_col=return_price_col,
            )

            if apply_coverage_filter and priced_weight < coverage_threshold:
                continue

            period_ret, realized_priced_weight, realized_priced_count, _ = period_return_for_portfolio(
                portfolio=candidate,
                price_map=price_map,
                period_start=q.rebalance_date,
                period_end=q.holding_period_end,
                trade_entry_col=trade_entry_col,
                return_price_col=return_price_col,
            )

            results.append(
                PeriodResult(
                    CIK=q.CIK,
                    FILINGMANAGER_NAME=fund_name,
                    holding_period_start=q.rebalance_date,
                    holding_period_end=q.holding_period_end,
                    holding_period_end_source=q.holding_period_end_source,
                    is_terminal_period=bool(q.is_terminal_period),
                    source_periodofreport=q.PERIODOFREPORT,
                    source_filing_date=q.FILING_DATE,
                    priced_weight=float(realized_priced_weight),
                    n_candidate_names=len(candidate),
                    n_priced_names=int(realized_priced_count),
                    period_return=float(period_ret) if pd.notna(period_ret) else np.nan,
                )
            )

    periods = pd.DataFrame([r.__dict__ for r in results])
    if periods.empty:
        raise ValueError("No backtest periods were generated. Check your data and filters.")

    periods = periods.sort_values(["CIK", "holding_period_start"]).reset_index(drop=True)

    summary_rows = []
    for cik, g in periods.groupby("CIK", sort=False):
        g = g.sort_values("holding_period_start").copy()
        valid = g["period_return"].dropna()
        n_periods = len(valid)
        if n_periods == 0:
            continue

        cumulative = float((1.0 + valid).prod() - 1.0)
        start_dt = g["holding_period_start"].min()
        end_dt = g["holding_period_end"].max()
        years = max((end_dt - start_dt).days / 365.25, 1e-9)
        periods_per_year = n_periods / years if years > 0 else np.nan

        cagr = float((1.0 + cumulative) ** (1.0 / years) - 1.0) if cumulative > -1 else -1.0
        mean_period = float(valid.mean())
        std_period = float(valid.std(ddof=1)) if len(valid) > 1 else np.nan
        annualized_vol = float(std_period * np.sqrt(periods_per_year)) if pd.notna(std_period) and pd.notna(periods_per_year) else np.nan
        annualized_sharpe_zero_rf = float(((mean_period - RISK_FREE_RATE) / std_period) * np.sqrt(periods_per_year)) if pd.notna(std_period) and std_period > 0 and pd.notna(periods_per_year) else np.nan
        max_dd = max_drawdown_from_returns(valid)
        calmar_ratio = float(cagr / abs(max_dd)) if pd.notna(max_dd) and max_dd < 0 else np.nan
        positive_period_rate = float((valid > 0).mean())

        summary_rows.append(
            {
                "CIK": cik,
                "FILINGMANAGER_NAME": g["FILINGMANAGER_NAME"].iloc[0],
                "n_periods": n_periods,
                "start_date": start_dt,
                "end_date": end_dt,
                "last_periodofreport": g["source_periodofreport"].max(),
                "last_filing_date": g["source_filing_date"].max(),
                "cumulative_return": cumulative,
                "CAGR": cagr,
                "mean_period_return": mean_period,
                "std_period_return": std_period,
                "avg_priced_weight": float(g["priced_weight"].mean()),
                "annualized_volatility": annualized_vol,
                "annualized_sharpe_zero_rf": annualized_sharpe_zero_rf,
                "max_drawdown": max_dd,
                "calmar_ratio": calmar_ratio,
                "positive_period_rate": positive_period_rate,
            }
        )

    summary = pd.DataFrame(summary_rows)
    summary = summary[summary["n_periods"] >= MIN_VALID_PERIODS_PER_FUND].copy()
    summary = summary.sort_values(["annualized_sharpe_zero_rf", "CAGR", "cumulative_return"], ascending=[False, False, False]).reset_index(drop=True)
    return periods, summary


# =========================================================
# MAIN
# =========================================================


def main() -> None:
    if DEBUG:
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/5] Reading and combining all 13F parquet files ...")
    df_raw = read_all_13f_data(FILINGS_DIR)

    print("[2/5] Filtering to US common stocks and cleaning 13F data ...")
    df_13f, universe_diagnostics = load_13f_data(df_raw)

    print("[3/5] Building fund-quarter holdings with US-only weights ...")
    holdings = build_quarter_holdings(df_13f)

    print("[4/5] Loading price data and deriving raw-open + adjusted-open series ...")
    price_map, trading_dates, trade_entry_col, return_price_col = load_price_data(PRICE_PATH)

    print("[5/5] Running backtest ...")
    periods, summary = run_backtest(
        holdings=holdings,
        price_map=price_map,
        trading_dates=trading_dates,
        trade_entry_col=trade_entry_col,
        return_price_col=return_price_col,
        apply_coverage_filter=APPLY_COVERAGE_FILTER,
        coverage_threshold=COVERAGE_THRESHOLD,
        backtest_end_date=BACKTEST_END_DATE,
    )

    periods_path = OUTPUT_DIR / "institution_backtest_periods_us_raw_open_checked_adj_open_returns.csv"
    summary_path = OUTPUT_DIR / "institution_backtest_summary_us_raw_open_checked_adj_open_returns.csv"
    holdings_path = OUTPUT_DIR / "institution_quarter_holdings_us.csv"
    diag_path = OUTPUT_DIR / "us_universe_diagnostics.csv"

    periods.to_csv(periods_path, index=False)
    summary.to_csv(summary_path, index=False)
    holdings.to_csv(holdings_path, index=False)
    universe_diagnostics.to_csv(diag_path, index=False)

    print("\nDone.")
    print(f"Entry availability check column used: {trade_entry_col} (raw market open)")
    print(f"Return measurement price column used: {return_price_col} (derived adjusted-open)")
    print("Economic return calculation is adjusted-open to adjusted-open.")
    print(f"Entry lag (trading days after filing): {ENTRY_LAG_TRADING_DAYS}")
    print(f"Terminal disclosed portfolios are carried forward to the backtest cut-off date: {BACKTEST_END_DATE.date()}")
    print(f"Coverage filter applied: {APPLY_COVERAGE_FILTER}")
    if APPLY_COVERAGE_FILTER:
        print(f"Coverage threshold: {COVERAGE_THRESHOLD:.0%}")
    print(f"Missing weight treated as cash: {MISSING_WEIGHT_AS_CASH}")
    print(f"Periods saved to: {periods_path}")
    print(f"Summary saved to: {summary_path}")
    print(f"US-only holdings saved to: {holdings_path}")
    print(f"Universe diagnostics saved to: {diag_path}")

    print("\nUniverse diagnostics:")
    print(universe_diagnostics.to_string(index=False))

    print("\nTop 20 institutions:")
    show_cols = [
        "CIK",
        "FILINGMANAGER_NAME",
        "n_periods",
        "last_periodofreport",
        "cumulative_return",
        "CAGR",
        "avg_priced_weight",
        "annualized_sharpe_zero_rf",
        "max_drawdown",
        "calmar_ratio",
    ]
    print(summary[show_cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
