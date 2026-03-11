import streamlit as st
import pandas as pd
import numpy as np

st.title("Backtesting Dashboard")

st.sidebar.header("Filters")


start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")

institutions = st.sidebar.multiselect(
    "Select Institutions",
    ["BlackRock", "Vanguard", "Bridgewater", "Renaissance"]
)

st.write("### Selected Filters")

st.write("Start date:", start_date)
st.write("End date:", end_date)
st.write("Institutions:", institutions)

# Example dummy data
dates = pd.date_range("2020-01-01", periods=100)
portfolio = np.cumprod(1 + np.random.normal(0.001, 0.02, 100))

df = pd.DataFrame({
    "date": dates,
    "portfolio_value": portfolio
})

st.line_chart(df.set_index("date"))