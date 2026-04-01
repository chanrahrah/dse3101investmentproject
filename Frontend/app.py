import sys
from pathlib import Path
from datetime import date

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

import streamlit as st
from components.portfolio_performance import portfolio_performance
from components.add_fees import add_fees
from components.top_20 import top_20_table
from Backend.backtesting.batch_process_rank_stocks import main

# page set up and layout
st.set_page_config(
    page_title="dse3101 project",
    layout="wide"
)

# title
c_title, c_backtest = st.columns([8, 2], vertical_alignment="center")
with c_title:
    st.title("Dashboard")

# date options
quarter_end_dates = [
    date(2024, 3, 31),
    date(2024, 6, 30),
    date(2024, 9, 30),
    date(2024, 12, 31),
]

# user input row
c1, c2, c3, c4, c5, c6, c7 = st.columns([0.18, 0.16, 0.16, 0.16, 0.12, 0.12, 0.10])

with c1:
    initial_capital = st.number_input(
        "Initial Capital ($)",
        min_value=0,
        value=10000,
        step=1000,
        key="initial_capital"
    )

with c2:
    cost_rate = add_fees()

with c3:
    from_date = st.selectbox(
        "From:",
        options=quarter_end_dates,
        index=0,
        format_func=lambda d: d.strftime("%Y-%m-%d"),
        key="from_date"
    )

with c4:
    valid_to_dates = [d for d in quarter_end_dates if d >= from_date]
    to_date = st.selectbox(
        "To:",
        options=valid_to_dates,
        index=len(valid_to_dates) - 1,
        format_func=lambda d: d.strftime("%Y-%m-%d"),
        key="to_date"
    )

with c5:
    topN = st.number_input(
        "Top N Stocks",
        min_value=1,
        max_value=50,
        value=10,
        step=1,
        key="topN"
    )

with c6:
    topN_institutions = st.selectbox(
        "Top N Institutions",
        options=[10, 20, 30],
        index=0,
        key="topN_institutions"
    )

with c7:
    lag = st.number_input(
        "Lag",
        min_value=0,
        value=47,
        step=1,
        key="lag"
    )

portfolio_df = None
metrics_df = None

try:
    with st.spinner("Running backtest..."):
        portfolio_df, metrics_df = main(
            userinput_start_date=from_date.strftime("%Y-%m-%d"),
            userinput_end_date=to_date.strftime("%Y-%m-%d"),
            userinput_initial_capital=float(initial_capital),
            userinput_topN_institutions=int(topN_institutions),
            userinput_topN_stocks=int(topN),
            userinput_lag=int(lag),
            userinput_cost_rate=float(cost_rate),
        )
except Exception as e:
    st.error(f"Error running backend: {e}")

# main layout
col_left, col_right = st.columns([6, 4])

with col_left:
    st.header("Portfolio Performance")
    portfolio_performance()

with col_right:
    st.header("Top Stocks by Institutional Holdings")
    if portfolio_df is not None:
        top_20_table(
            portfolio_df,
            top_n=int(topN),
            selected_quarter=from_date.strftime("%Y-%m-%d")
        )
    else:
        st.info("No holdings data yet.")