import streamlit as st

st.set_page_config(page_title="Institutional Copytrade Platform")

st.title("Institutional Holdings Copytrade Platform")

st.write("""
Welcome to our investment analytics dashboard.

This platform allows you to:
- View institutional holdings
- Backtest copy-trading strategies
- Compare portfolio performance with benchmarks
""")

st.header("How it works")

st.write("""
1. Institutional holdings are extracted from SEC 13F filings  
2. Portfolios are reconstructed quarterly  
3. Strategies are backtested with realistic transaction costs
""")