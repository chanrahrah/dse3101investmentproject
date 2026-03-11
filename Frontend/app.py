import streamlit as st
from components.new_backtest import new_backtest_button, new_backtest_panel #for yenfay's code testing, DO NOT OVERWRITE PLEASE! 

st.set_page_config(
    page_title="Institutional Copytrade Platform",
    layout="wide"
)

st.title("Beginner Dashboard")

st.write("Explore institutional copy-trading strategies.")