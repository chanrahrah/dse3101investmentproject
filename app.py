from datetime import date
from dateutil.relativedelta import relativedelta
import calendar
from pathlib import Path
import sys

from datetime import date
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

from Backend.backtesting.batch_process_rank_stocks import main
from Frontend.components.cumulative_returns import cumulative_returns
from Frontend.components.daily_returns import daily_returns
from Frontend.components.performance_metrics import performance_metrics
from Frontend.components.top_20 import render_stock_details, top_20_table
from Frontend.components.portfolio_performance import portfolio_performance


STOCK_SNAPSHOT_PATH = ROOT_DIR / "Datasets" / "final_files" / "stock_snapshot.parquet"
SPY_PATH = ROOT_DIR / "Datasets" / "final_files" / "spy_prices_2013-01-01_to_2026-03-31.parquet"

stock_snapshot_df = pd.read_parquet(STOCK_SNAPSHOT_PATH) if STOCK_SNAPSHOT_PATH.exists() else None
spy_df = pd.read_parquet(SPY_PATH) if SPY_PATH.exists() else None

# macro page configurations 
st.set_page_config(page_title="dse3101 project", layout="wide")
st.title("Dashboard")


c1, c2, c3, c4, c5, c6 = st.columns([0.20, 0.18, 0.18, 0.18, 0.13, 0.13])

with c1:
    initial_capital = st.number_input(
        "Initial Capital ($)",
        min_value=0,
        value=10000,
        step=1000,
        key="initial_capital",)

with c2:
    cost_rate = st.number_input(
        "Fees per dollar value of transaction ($)",
        min_value=0.0,
        value=0.001,
        step=0.001,
        format="%.3f",
        key="fee_per_trade",)

MIN_START_DATE = date(2013, 8, 16)
MAX_END_DATE = date(2026, 3, 31)

with c3:
    from_date = st.date_input(
        "From:",
        value=MIN_START_DATE,
        min_value=MIN_START_DATE,
        max_value=MAX_END_DATE,
        key="from_date",
        format="YYYY-MM-DD",
    )
    st.caption("Earliest date: 2018-08-16")

min_to_date = from_date + relativedelta(months=+6)

if min_to_date > MAX_END_DATE:
    st.error("No valid end date available. Please choose an earlier start date.")
    st.stop()

with c4:
    default_to_date = MAX_END_DATE if MAX_END_DATE >= min_to_date else min_to_date

    to_date = st.date_input(
        "To:",
        value=default_to_date,
        min_value=min_to_date,
        max_value=MAX_END_DATE,
        key="to_date",
        format="YYYY-MM-DD",
    )
    st.caption("Latest date: 2026-03-31")

if to_date < min_to_date:
    st.error("End date must be at least 6 months after the start date.")
    st.stop()

with c5:
    topN = st.number_input(
        "Top N Stocks",
        min_value=1,
        max_value=50,
        value=10,
        step=1,
        key="topN",)

with c6:
    topN_institutions = st.selectbox(
        "Top N Institutions",
        options=[10, 20, 30],
        index=0,
        key="topN_institutions",)


portfolio_df = None
metrics_df = None

try:
    with st.spinner("Running backtest..."):
        portfolio_df, metrics_df = main(
            userinput_start_date=from_date.strftime("%Y-%m-%d"),
            userinput_end_date=to_date.strftime("%Y-%m-%d"),
            userinput_initial_capital=float(initial_capital),
            userinput_topN_stocks=int(topN),
            userinput_cost_rate=float(cost_rate),)

        if portfolio_df is not None and spy_df is not None:
            portfolio_df = portfolio_df.copy()
            portfolio_df["date"] = pd.to_datetime(portfolio_df["date"]).dt.normalize()

            spy_merge = spy_df.copy()
            spy_merge["date"] = pd.to_datetime(spy_merge["date"]).dt.normalize()

            spy_merge = (
                spy_merge[spy_merge["ticker"].astype(str).str.upper() == "SPY"][["date", "adj_close"]]
                .drop_duplicates(subset=["date"])
                .rename(columns={"adj_close": "spy_price"}))

            portfolio_df = portfolio_df.merge(spy_merge, on="date", how="left")

            valid_spy = portfolio_df["spy_price"].dropna()
            if not valid_spy.empty:
                first_spy_price = valid_spy.iloc[0]
                portfolio_df["spy_value"] = (
                    portfolio_df["spy_price"] / first_spy_price) * float(initial_capital)

except Exception as e:
    st.error(f"Error running backend: {e}")


col_left, col_right = st.columns([7.7, 2.3])
selected_tickers = None


with col_left:
    tab1, tab2, tab3 = st.tabs(["Portfolio Performance", "Daily Returns", "Cumulative Returns"])

    with tab1:
        if portfolio_df is not None:
            portfolio_performance(portfolio_df)
        else:
            st.warning("Portfolio Performance component could not be loaded.")

    with tab2:
        if portfolio_df is not None:
            daily_returns(portfolio_df)
        else:
            st.info("No portfolio data available yet.")

    with tab3:
        if portfolio_df is not None:
            cumulative_returns(portfolio_df)
        else:
            st.info("No portfolio data available yet.")

    st.markdown("---")
    st.header("Performance Metrics")
    performance_metrics(portfolio_df, metrics_df)


with col_right:
    if portfolio_df is not None:
        selected_tickers = top_20_table(
            portfolio_df,
            top_n=int(topN),
            top_m_institutions=int(topN_institutions),
            fee_per_dollar=float(cost_rate),)
    else:
        st.info("No holdings data yet.")

# bottom section
st.markdown("---")
st.header("Stock Details")
render_stock_details(selected_tickers, stock_snapshot_df)