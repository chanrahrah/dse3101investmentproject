import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from components.top_20 import top_20_table, render_stock_details
from Backend.backtesting.batch_process_rank_stocks import main

try:
    from components.portfolio_performance import portfolio_performance
    portfolio_performance_import_error = None
except Exception as e:
    portfolio_performance = None
    portfolio_performance_import_error = e

STOCK_SNAPSHOT_PATH = ROOT_DIR / "Datasets" / "final_files" / "stock_snapshot.parquet"

stock_snapshot_df = None
if STOCK_SNAPSHOT_PATH.exists():
    stock_snapshot_df = pd.read_parquet(STOCK_SNAPSHOT_PATH)


try:
    from components.portfolio_performance import portfolio_performance
    portfolio_performance_import_error = None
except Exception as e:
    portfolio_performance = None
    portfolio_performance_import_error = e


def get_available_quarter_dates():
    possible_files = [
        ROOT_DIR / "Datasets" / "final_files" / "final_top10_form13f.parquet",
        ROOT_DIR / "Datasets" / "final_files" / "final_top20_form13f.parquet",
        ROOT_DIR / "Datasets" / "final_files" / "final_top30_form13f.parquet",
    ]

    holdings_df = None
    for file_path in possible_files:
        if file_path.exists():
            holdings_df = pd.read_parquet(file_path)
            break

    if holdings_df is None:
        raise FileNotFoundError(
            "No form13f parquet file found in Datasets/final_files."
        )

    holdings_df.columns = [col.lower() for col in holdings_df.columns]

    if "periodofreport" not in holdings_df.columns:
        raise ValueError(
            f"'PERIODOFREPORT' column not found. Columns are: {holdings_df.columns.tolist()}"
        )

    parsed_dates = pd.to_datetime(
        holdings_df["periodofreport"], errors="coerce"
    ).dropna()

    if parsed_dates.empty:
        raise ValueError("Could not parse any valid dates from PERIODOFREPORT.")

    quarter_dates = sorted(
        {
            d.date()
            for d in parsed_dates
            if (d.month, d.day) in [(3, 31), (6, 30), (9, 30), (12, 31)]
        }
    )

    return quarter_dates


st.set_page_config(page_title="dse3101 project", layout="wide")
st.title("Dashboard")


try:
    quarter_end_dates = get_available_quarter_dates()
except Exception as e:
    st.error(f"Error loading available quarter dates: {e}")
    st.stop()

if len(quarter_end_dates) < 2:
    st.error("Not enough available quarter dates found in backend data.")
    st.stop()

c1, c2, c3, c4, c5, c6 = st.columns([0.20, 0.18, 0.18, 0.18, 0.13, 0.13])
with c1:
    initial_capital = st.number_input(
        "Initial Capital ($)",
        min_value=0,
        value=10000,
        step=1000,
        key="initial_capital",
    )

with c2:
    cost_rate = st.number_input(
        "Fees per dollar value of transaction ($)",
        min_value=0.0,
        value=0.0,
        step=0.1,
        key="fee_per_trade",
    )

with c3:
    start_date_options = quarter_end_dates[:-1]
    from_date = st.selectbox(
        "From:",
        options=start_date_options,
        index=0,
        format_func=lambda d: d.strftime("%Y-%m-%d"),
        key="from_date",
    )

with c4:
    valid_to_dates = [d for d in quarter_end_dates if d > from_date]
    to_date = st.selectbox(
        "To:",
        options=valid_to_dates,
        index=len(valid_to_dates) - 1,
        format_func=lambda d: d.strftime("%Y-%m-%d"),
        key="to_date",
    )

with c5:
    topN = st.number_input(
        "Top N Stocks",
        min_value=1,
        max_value=50,
        value=10,
        step=1,
        key="topN",
    )

with c6:
    topN_institutions = st.selectbox(
        "Top N Institutions",
        options=[10, 20, 30],
        index=0,
        key="topN_institutions",
    )

portfolio_df = None
metrics_df = None

try:
    with st.spinner("Running backtest..."):
        portfolio_df, metrics_df = main(
            userinput_start_date=from_date.strftime("%Y-%m-%d"),
            userinput_end_date=to_date.strftime("%Y-%m-%d"),
            userinput_initial_capital=float(initial_capital),
            userinput_topN_stocks=int(topN),
        )
except Exception as e:
    st.error(f"Error running backend: {e}")

# main layout
col_left, col_right = st.columns([3, 1])

selected_tickers = None

with col_left:
    st.header("Portfolio Performance")
    if portfolio_performance is not None:
        portfolio_performance()
    else:
        st.warning("Portfolio Performance component could not be loaded.")
        st.caption(str(portfolio_performance_import_error))

with col_right:
    st.header("Top Stocks by Institutional Holdings")
    if portfolio_df is not None:
        selected_tickers = top_20_table(
            portfolio_df,
            top_n=int(topN),
            selected_quarter=from_date.strftime("%Y-%m-%d")
        )
    else:
        st.info("No holdings data yet.")

st.markdown("---")
st.header("Stock Details")
render_stock_details(selected_tickers, stock_snapshot_df)