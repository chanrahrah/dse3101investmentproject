import streamlit as st
import pandas as pd

# create button
def new_backtest_button():
    if st.button("➕ New Backtest"):
        show_backtest_modal()
        
# add new backtest
@st.dialog("New Backtest")
def show_backtest_modal():

    st.markdown("# Date")
    col_start, col_end = st.columns([1, 1])
    with col_start:
        from_date = st.date_input("From:", key="backtest_from_date")
    with col_end:
        to_date = st.date_input("To:", key="backtest_to_date")
        
        from datetime import date

    st.markdown("# Strategy")
    ticker = st.text_input("Ticker")

    st.markdown("# Portfolio Allocation")
    
    df = pd.DataFrame(columns=["Ticker", "Allocation (%)", "Recommended Allocation (%)"])
    edited_df = st.data_editor(df, num_rows="dynamic")

    st.markdown("# Allocation")
    starting_funds = st.number_input("Starting Funds", value=10000)
    margin = st.number_input("Margin Allocation per Trade (%)", value=10)
    max_positions = st.number_input("Max Open Positions", value=5)
    
    col1, col2 = st.columns([9, 1.23])

    with col1:
        if st.button("Cancel"):
            st.rerun()

    with col2:
        if st.button("Run"):
            st.success("Backtest started!")
            st.rerun()