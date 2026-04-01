import streamlit as st
import pandas as pd

DUMMY_STOCK_DETAILS = {
    "AAPL": {
        "Market Cap": "3.4T",
        "PE": "31.2",
        "PB": "45.1",
        "52 Week High": "260.1",
        "52 Week Low": "164.08",
        "Historic High": "260.1",
        "Historic Low": "12.5",
        "Dividend % TTM": "0.43%",
    },
    "MSFT": {
        "Market Cap": "3.1T",
        "PE": "36.8",
        "PB": "12.9",
        "52 Week High": "468.3",
        "52 Week Low": "309.4",
        "Historic High": "468.3",
        "Historic Low": "18.8",
        "Dividend % TTM": "0.68%",
    },
    "NVDA": {
        "Market Cap": "2.8T",
        "PE": "65.4",
        "PB": "58.7",
        "52 Week High": "153.1",
        "52 Week Low": "45.2",
        "Historic High": "153.1",
        "Historic Low": "0.35",
        "Dividend % TTM": "0.03%",
    },
}

DEFAULT_DUMMY_DETAILS = {
    "Market Cap": "N/A",
    "PE": "N/A",
    "PB": "N/A",
    "52 Week High": "N/A",
    "52 Week Low": "N/A",
    "Historic High": "N/A",
    "Historic Low": "N/A",
    "Dividend % TTM": "N/A",
}

def top_20_table(portfolio_df, top_n=10, selected_quarter=None):
    if portfolio_df is None or portfolio_df.empty:
        st.info("No holdings data available.")
        return

    quarter_df = portfolio_df.drop_duplicates(subset=["quarter"]).copy()

    if "tickers" not in quarter_df.columns or quarter_df.empty:
        st.info("No ticker data available.")
        return

    quarter_df["quarter"] = quarter_df["quarter"].astype(str)

    if selected_quarter is not None and selected_quarter in quarter_df["quarter"].values:
        selected_row = quarter_df[quarter_df["quarter"] == selected_quarter].iloc[0]
        st.caption(f"Showing selected quarter: {selected_quarter}")
    else:
        quarter_df = quarter_df.sort_values("quarter")
        selected_row = quarter_df.iloc[-1]
        st.caption(f"Showing latest available quarter: {selected_row['quarter']}")

    tickers = selected_row["tickers"]

    if not isinstance(tickers, list) or len(tickers) == 0:
        st.info("No tickers available for this quarter.")
        return

    tickers = tickers[:top_n]

    display_df = pd.DataFrame({
        "Rank": range(1, len(tickers) + 1),
        "Ticker": tickers
    })

    visible_rows = min(len(display_df), 20)
    row_height = 32
    header_height = 35
    table_height = header_height + visible_rows * row_height

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        height=table_height
    )

    st.markdown("---")

    selected_ticker = st.selectbox(
        "Select a stock to view more details:",
        tickers,
        key="selected_ticker_details"
    )

    details = DUMMY_STOCK_DETAILS.get(selected_ticker, DEFAULT_DUMMY_DETAILS)

    st.subheader(f"{selected_ticker} Details")

    c1, c2 = st.columns(2)

    with c1:
        st.metric("Market Cap", details["Market Cap"])
        st.metric("PE", details["PE"])
        st.metric("52 Week High", details["52 Week High"])
        st.metric("52 Week Low", details["52 Week Low"])

    with c2:
        st.metric("PB", details["PB"])
        st.metric("Historic High", details["Historic High"])
        st.metric("Historic Low", details["Historic Low"])
        st.metric("Dividend % TTM", details["Dividend % TTM"])