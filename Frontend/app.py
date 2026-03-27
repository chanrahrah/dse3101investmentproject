import streamlit as st
from components.portfolio_performance import portfolio_performance
from components.add_fees import add_fees
from components.top_20 import top_20_table 
from datetime import date 

# page set up and layout
st.set_page_config(
    page_title="dse3101 project",
    layout="wide"
)

# title and new layout of buttons
c_title, c_backtest = st.columns([8, 2], vertical_alignment="center")

with c_title:
    st.title("Dashboard")
    
# date layout
c1, c2, c3, c4 = st.columns([0.5, 0.2, 0.15, 0.15])
quarter_end_dates = [
    date(2025, 3, 31),
    date(2025, 6, 30),
    date(2025, 9, 30),
    date(2025, 12, 31),
]

with c1:
    st.write("")

with c2:
    fee_per_trade = add_fees()

# from date
with c3:
    from_date = st.selectbox(
        "From:",
        options=quarter_end_dates,
        index=0,
        format_func=lambda d: d.strftime("%Y/%m/%d"),
        key="from_date"
    )

# to date
with c4:
    valid_to_dates = [d for d in quarter_end_dates if d >= from_date]

    to_date = st.selectbox(
        "To:",
        options=valid_to_dates,
        index=len(valid_to_dates) - 1,
        format_func=lambda d: d.strftime("%Y/%m/%d"),
        key="to_date"
    )

# configure left column for portfolio performance and right column for top 20 table
col_left, col_right = st.columns([6, 4])

with col_left:
    st.header("Porfolio performance")
    portfolio_performance()
    
with col_right:
    st.header("Top 20 Stocks by Institutional Holdings")
    top_20_table()