from pathlib import Path 
import logging
import pandas as pd
import numpy as np
import time
from Backend.backtesting.filter_institutions_and_prices_helper_functions import (
    filter_form13f_for_top_institutions,
    filter_prices_for_top_institutions,
)
from Backend.backtesting.rank_stocks_helper_functions import (
    load_holdings,
    load_prices,
    filter_dates,
    aggregate_stock_weights,
    rank_topN,
    apply_filing_lag_and_get_trade_prices,
    extract_price_subset,
    run_backtest,
    get_spy_df,
)

# ==========================================================
# STANDARD PATHS AND CONFIG
# ==========================================================
from config import (
    DEBUG,
    KAGGLE_KEY,
    KAGGLE_USERNAME,
    OPENFIGI_KEY,
    OPENFIGI_URL,
    DATA_DIR,
    FORM13F_FOLDER_PATH,
    PRICES_FILE_FULL,
    SPY_DATA_DIR,
    FINAL_FILES_FOLDER,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ===========================================================
# 1. Get final form13f files and final price files (filtered to contain only top 10/20/30 institutions based on user inputs)
# ===========================================================
def get_final_files(top_n_institutions: list[str], form13f_folder_path: Path, prices_file_path: Path, output_folder: Path):
    # Filter the full form13f data for top_N institutions
    logger.info("[INFO] Filtering full form13f data and stock price data for TOP N Institutions")
    filter_form13f_for_top_institutions(form13f_folder_path, top_n_institutions, output_folder)

    # Filter the full stock price data to only include stocks held by the top_N institutions 
    num_institutions = len(top_n_institutions)
    holdings_file = f"{output_folder}/final_top{num_institutions}_form13f.parquet"
    filter_prices_for_top_institutions(top_n_institutions, prices_file_path, holdings_file, output_folder)
    logger.info("[INFO] Filtering DONE.")

# ===========================================================
# 2. Run strategy
# ===========================================================
def run_strategy(
    final_files_folder: Path,
    start_date: str,
    end_date: str,
    initial_capital: float,
    topN_institutions: int = 10,
    topN_stocks: int = 10,
    cost_rate: float = 0.001,
) -> "pd.DataFrame":
    """
    End-to-end 13F copycat back-test.

    Parameters
    ----------
    holdings_file_path   : path to file containing form13f holdings data for all top institutions
    prices_file_path     : path to consolidated stock-price parquet file
    start_date           : inclusive start of PERIODOFREPORT filter  (YYYY-MM-DD)
    end_date             : inclusive end   of PERIODOFREPORT filter  (YYYY-MM-DD)
    initial_capital      : starting portfolio value in dollars
    lag                  : calendar days after PERIODOFREPORT before we trade (default 47)
                           trade_date = PERIODOFREPORT + lag, snapped back to prev trading day
    cost_rate            : transaction cost as a fraction of traded dollar value (default 0.001 = 0.1%)

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
    logger.info("[INFO] Running strategy START")
    # 1. Dynamically load holdings and price data based on userinput_topM_institutions
    holdings = load_holdings(final_files_folder/f"final_top{topN_institutions}_form13f.parquet")
    logger.info("[INFO] Holdings loaded")
    prices   = load_prices(final_files_folder/f"final_top{topN_institutions}_prices.parquet")
    logger.info("[INFO] Prices loaded")

    # 2. Filter holdings to the requested date range
    holdings = filter_dates(holdings, start_date, end_date)
    logger.info("[INFO] Filtered holdings to userinput dates")

    # 3. Aggregate cross-institution weights and pick top N stocks per quarter
    agg   = aggregate_stock_weights(holdings)
    logger.info("[INFO] Stock weights aggregated")
    topN = rank_topN(agg, topN_stocks)
    logger.info("[INFO] Stocks ranked and top {topN_stocks} extracted per quarter")

    # 4. Compute candidate trade_date = earliest FILING_DATE + lag + get trade prices
    topN = apply_filing_lag_and_get_trade_prices(topN, prices)
    logger.info("[INFO] Lagging applied and trade prices obtained")

    # 5. Narrow prices to only the tickers we actually hold (memory efficiency)
    prices_subset = extract_price_subset(prices, topN)
    logger.info("[INFO] Extracted price subset")

    # 6. Run the back-test
    logger.info("[INFO] Backtesting START")
    portfolio = run_backtest(topN, prices_subset, initial_capital, cost_rate=cost_rate, start_date=start_date, end_date=end_date)

    logger.info("[INFO] Backtesting END")
    logger.info("[INFO] Portoflio dataframe obtained")


    return portfolio

# ===========================================================
# 3. Get comparison df of strategy vs SPY
# ===========================================================
def run_comparision(portfolio_df: pd.DataFrame, spy_file_path: Path, start_date: str, end_date: str, initial_capital: float) -> pd.DataFrame:
    logger.info("[INFO] Running comparison of strategy vs SPY START")
    raw_spy_df = pd.read_parquet(spy_file_path)
    spy_df = get_spy_df(raw_spy_df, start_date, end_date, initial_capital)

    # Ensure date columns are same type before merging
    portfolio_df["date"] = pd.to_datetime(portfolio_df["date"])
    spy_df["date"] = pd.to_datetime(spy_df["date"])

    # Merge portfolio and spy_df by date
    comparison_df = portfolio_df.merge(spy_df[["date", "spy_daily_return", "spy_cum_return", "spy_portfolio_value"]], on="date", how="left")
    
    logger.info("[INFO] Comparison dataframe obtained")
    return comparison_df

# ===========================================================
# MAIN RUN
# ===========================================================
def main(userinput_start_date, 
         userinput_end_date, 
         userinput_initial_capital,
         userinput_topM_institutions = 10, 
         userinput_topN_stocks = 10,
         userinput_cost_rate = 0.001):
    
    # To track time taken to run main()
    start = time.perf_counter()

    # 1. run strategy
    portfolio_df = run_strategy(
        final_files_folder=FINAL_FILES_FOLDER,
        start_date=userinput_start_date,
        end_date=userinput_end_date,
        initial_capital = userinput_initial_capital,
        topN_institutions = userinput_topM_institutions,
        topN_stocks = userinput_topN_stocks,
        cost_rate=userinput_cost_rate
    )

    # 2. get comparison df of strategy vs SPY
    full_df = run_comparision(portfolio_df, SPY_DATA_DIR, userinput_start_date, userinput_end_date, userinput_initial_capital)
    
    logging.info(f"Time taken to run main(): {time.perf_counter() - start:.2f}s")

    return portfolio_df, full_df
# ---------------------------------------------------------------------------
# RUN
# ---------------------------------------------------------------------------
if __name__ == "__main__":

    if not DEBUG:
        # Perform all the steps only in production mode. 
        # Get final files for top 10 institutions
        TOP_10_INSTITUTIONS = ['0000914976','0001767601','0001482935','0001697233','0000872259','0001536446','0001017115','0001641447','0001727993','0001502149']
        get_final_files(TOP_10_INSTITUTIONS, FORM13F_FOLDER_PATH, PRICES_FILE_FULL, FINAL_FILES_FOLDER)
        # Get final files for top 20 institutions
        TOP_20_INSTITUTIONS = ['0000914976','0001767601','0001482935','0001697233','0000872259','0001536446','0001017115','0001641447','0001727993','0001502149',
                               '0001308685','0001641643','0001080369','0001455251','0001767898','0001082339','0001004244','0001698777','0001592746','0001510434']
        get_final_files(TOP_20_INSTITUTIONS, FORM13F_FOLDER_PATH, PRICES_FILE_FULL, FINAL_FILES_FOLDER)
        # Get final files for top 30 institutions
        TOP_30_INSTITUTIONS = ['0000914976','0001767601','0001482935','0001697233','0000872259','0001536446','0001017115','0001641447','0001727993','0001502149',
                               '0001308685','0001641643','0001080369','0001455251','0001767898','0001082339','0001004244','0001698777','0001592746','0001510434',
                               '0001667134','0001120048','0001600999','0001020580','0001764387','0001747799','0001512026','0001054646','0000866590','0001033225']
        get_final_files(TOP_30_INSTITUTIONS, FORM13F_FOLDER_PATH, PRICES_FILE_FULL, FINAL_FILES_FOLDER)

    # Change user inputs here:
    userinput_start_date = '2015-04-01' 
    userinput_end_date = '2025-11-20'
    userinput_initial_capital = 10_000
    userinput_topM_institutions= 20 # default at 10. User can choose between 10,20,30 top institutions.
    userinput_topN = 18 # default at 10. User can choose any topN stocks to hold per quarter.
    userinput_cost_rate = 0.001  # default at 0.001. transaction cost as a fraction of traded dollar value (default 0.001 = 0.1%)

    portfolio_df, full_df = main(userinput_start_date, 
                                    userinput_end_date, 
                                    userinput_initial_capital,
                                    userinput_topM_institutions,
                                    userinput_topN,
                                    userinput_cost_rate)
    

    # To check the outputs on excel
    portfolio_df.to_csv((DATA_DIR / "data_for_frontend" / "portfolio.csv"))
    full_df.to_csv((DATA_DIR / "data_for_frontend" / "full.csv"))
