import streamlit as st
import pandas as pd
import plotly.express as px


def top_20_table():
    # Replace this dummy data with backend data
    holdings_data = [
        {
            "Rank": 1,
            "Ticker": "AAPL",
            "Allocation": 12.5,
            "Market Cap": "3.4T",
            "PE": 31.2,
            "52W High": 260.10,
            "52W Low": 164.08,
            "PB": 45.1,
            "Historic High": 260.10,
            "Historic Low": 12.50,
            "Dividend % TTM": "0.43%"
        },
        {
            "Rank": 2,
            "Ticker": "MSFT",
            "Allocation": 11.8,
            "Market Cap": "3.1T",
            "PE": 35.4,
            "52W High": 468.35,
            "52W Low": 309.45,
            "PB": 13.9,
            "Historic High": 468.35,
            "Historic Low": 15.15,
            "Dividend % TTM": "0.68%"
        },
        {
            "Rank": 3,
            "Ticker": "NVDA",
            "Allocation": 11.5,
            "Market Cap": "2.8T",
            "PE": 64.8,
            "52W High": 153.13,
            "52W Low": 75.61,
            "PB": 56.7,
            "Historic High": 153.13,
            "Historic Low": 0.35,
            "Dividend % TTM": "0.03%"
        },
        {
            "Rank": 4,
            "Ticker": "AMZN",
            "Allocation": 10.2,
            "Market Cap": "1.9T",
            "PE": 52.6,
            "52W High": 201.20,
            "52W Low": 118.35,
            "PB": 8.7,
            "Historic High": 201.20,
            "Historic Low": 5.97,
            "Dividend % TTM": "0.00%"
        },
        {
            "Rank": 5,
            "Ticker": "META",
            "Allocation": 9.4,
            "Market Cap": "1.3T",
            "PE": 29.7,
            "52W High": 638.40,
            "52W Low": 414.50,
            "PB": 9.8,
            "Historic High": 638.40,
            "Historic Low": 17.55,
            "Dividend % TTM": "0.33%"
        },
        {
            "Rank": 6,
            "Ticker": "GOOGL",
            "Allocation": 8.6,
            "Market Cap": "2.0T",
            "PE": 27.1,
            "52W High": 191.75,
            "52W Low": 121.46,
            "PB": 7.2,
            "Historic High": 191.75,
            "Historic Low": 2.47,
            "Dividend % TTM": "0.47%"
        },
        {
            "Rank": 7,
            "Ticker": "BRK.B",
            "Allocation": 6.0,
            "Market Cap": "960B",
            "PE": 10.8,
            "52W High": 491.67,
            "52W Low": 395.66,
            "PB": 1.6,
            "Historic High": 491.67,
            "Historic Low": 19.80,
            "Dividend % TTM": "0.00%"
        },
        {
            "Rank": 8,
            "Ticker": "JPM",
            "Allocation": 5.9,
            "Market Cap": "610B",
            "PE": 13.5,
            "52W High": 248.15,
            "52W Low": 172.62,
            "PB": 2.1,
            "Historic High": 248.15,
            "Historic Low": 15.26,
            "Dividend % TTM": "2.11%"
        },
        {
            "Rank": 9,
            "Ticker": "LLY",
            "Allocation": 4.3,
            "Market Cap": "720B",
            "PE": 71.3,
            "52W High": 972.53,
            "52W Low": 711.40,
            "PB": 52.0,
            "Historic High": 972.53,
            "Historic Low": 1.63,
            "Dividend % TTM": "0.52%"
        },
        {
            "Rank": 10,
            "Ticker": "XOM",
            "Allocation": 3.2,
            "Market Cap": "510B",
            "PE": 13.2,
            "52W High": 126.34,
            "52W Low": 95.77,
            "PB": 2.0,
            "Historic High": 126.34,
            "Historic Low": 1.50,
            "Dividend % TTM": "3.18%"
        }
    ]

    table_df = pd.DataFrame(holdings_data)[["Rank", "Ticker", "Allocation"]]

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True
    )

    st.markdown("---")

    # TODO: Replace ticker detail retrieval with backend-linked stock metadata if needed.
    ticker_options = table_df["Ticker"].tolist()

    selected_ticker = st.selectbox(
        "Select a stock to view more details:",
        options=ticker_options,
        key="selected_ticker"
    )

    selected_stock = next(
        stock for stock in holdings_data if stock["Ticker"] == selected_ticker
    )

    st.markdown(f"### {selected_stock['Ticker']} Details")

    col1, col2 = st.columns(2)

    with col1:
        st.metric("Market Cap", selected_stock["Market Cap"])
        st.metric("PE", selected_stock["PE"])
        st.metric("52 Week High", selected_stock["52W High"])
        st.metric("52 Week Low", selected_stock["52W Low"])

    with col2:
        st.metric("PB", selected_stock["PB"])
        st.metric("Historic High", selected_stock["Historic High"])
        st.metric("Historic Low", selected_stock["Historic Low"])
        st.metric("Dividend % TTM", selected_stock["Dividend % TTM"])
