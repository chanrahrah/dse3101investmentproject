import streamlit as st
import pandas as pd


def format_value(value, value_type="text"):
    if pd.isna(value):
        return "N/A"

    if value_type == "market_cap":
        abs_value = abs(value)
        if abs_value >= 1_000_000_000_000:
            return f"{value / 1_000_000_000_000:.2f}T"
        elif abs_value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        elif abs_value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        else:
            return f"{value:,.0f}"

    if value_type == "volume":
        abs_value = abs(value)
        if abs_value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        elif abs_value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        elif abs_value >= 1_000:
            return f"{value / 1_000:.2f}K"
        else:
            return f"{value:,.0f}"

    if value_type in ["price", "ratio", "beta", "eps", "target"]:
        return f"{value:.2f}"

    if value_type == "percent":
        if value > 1:
            return f"{value:.2f}%"
        return f"{value * 100:.2f}%"

    if value_type == "date":
        try:
            dt = pd.to_datetime(value, unit="s")
            if dt.year <= 1971:
                return "N/A"
            return dt.strftime("%Y-%m-%d")
        except Exception:
            try:
                dt = pd.to_datetime(value)
                if dt.year <= 1971:
                    return "N/A"
                return dt.strftime("%Y-%m-%d")
            except Exception:
                return "N/A"

    return str(value)


def get_stock_details(selected_ticker, stock_snapshot_df):
    default_details = {
        "Market Cap": "N/A",
        "PE Ratio": "N/A",
        "EPS": "N/A",
        "Beta": "N/A",
        "Forward Dividend Yield": "N/A",
        "Current Price": "N/A",
        "Previous Close": "N/A",
        "1Y Target Est": "N/A",
        "52 Week High": "N/A",
        "52 Week Low": "N/A",
        "Day High": "N/A",
        "Day Low": "N/A",
        "Volume": "N/A",
        "Avg Volume": "N/A",
        "Bid": "N/A",
        "Ask": "N/A",
        "Exchange Country": "N/A",
        "Earnings Date": "N/A",
        "Ex-Dividend Date": "N/A",
    }

    if stock_snapshot_df is None or stock_snapshot_df.empty:
        return default_details

    stock_row = stock_snapshot_df[stock_snapshot_df["ticker"] == selected_ticker]
    if stock_row.empty:
        return default_details

    stock_row = stock_row.iloc[0]

    return {
        "Market Cap": format_value(stock_row.get("market_cap"), "market_cap"),
        "PE Ratio": format_value(stock_row.get("pe_ratio"), "ratio"),
        "EPS": format_value(stock_row.get("eps"), "eps"),
        "Beta": format_value(stock_row.get("beta"), "beta"),
        "Forward Dividend Yield": format_value(stock_row.get("forward_dividend_yield"), "percent"),
        "Current Price": format_value(stock_row.get("close"), "price"),
        "Previous Close": format_value(stock_row.get("previous_close"), "price"),
        "1Y Target Est": format_value(stock_row.get("one_year_target_est"), "target"),
        "52 Week High": format_value(stock_row.get("fifty_two_week_high"), "price"),
        "52 Week Low": format_value(stock_row.get("fifty_two_week_low"), "price"),
        "Day High": format_value(stock_row.get("day_high"), "price"),
        "Day Low": format_value(stock_row.get("day_low"), "price"),
        "Volume": format_value(stock_row.get("volume"), "volume"),
        "Avg Volume": format_value(stock_row.get("avg_volume"), "volume"),
        "Bid": format_value(stock_row.get("bid"), "price"),
        "Ask": format_value(stock_row.get("ask"), "price"),
        "Exchange Country": format_value(stock_row.get("exchange_country")),
        "Earnings Date": format_value(stock_row.get("earnings_date"), "date"),
        "Ex-Dividend Date": format_value(stock_row.get("ex_dividend_date"), "date"),
    }


def top_20_table(portfolio_df, top_n=10, selected_quarter=None):
    if portfolio_df is None or portfolio_df.empty:
        st.info("No holdings data available.")
        return None

    quarter_df = portfolio_df.drop_duplicates(subset=["quarter"]).copy()

    if "tickers" not in quarter_df.columns or quarter_df.empty:
        st.info("No ticker data available.")
        return None

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
        return None

    tickers = tickers[:top_n]

    display_df = pd.DataFrame({
        "Rank": range(1, len(tickers) + 1),
        "Ticker": tickers
    })

    row_height = 35
    header_height = 38
    max_visible_rows = 20

    visible_rows = min(len(display_df), max_visible_rows)
    table_height = header_height + visible_rows * row_height

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        height=table_height
    )

    return tickers


def render_stock_details(tickers, stock_snapshot_df):
    if not tickers:
        st.info("Select a stock to view more details.")
        return

    selected_ticker = st.selectbox(
        "Select a stock to view more details:",
        tickers,
        key="selected_ticker_details"
    )

    details = get_stock_details(selected_ticker, stock_snapshot_df)

    st.markdown(f"**{selected_ticker} Details**")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Market Cap", details["Market Cap"])
    c2.metric("PE Ratio", details["PE Ratio"])
    c3.metric("Earnings Per Share", details["EPS"])
    c4.metric("Beta", details["Beta"])

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Forward Dividend Yield", details["Forward Dividend Yield"])
    c6.metric("Current Price", details["Current Price"])
    c7.metric("Previous Close", details["Previous Close"])
    c8.metric("1Y Target", details["1Y Target Est"])

    c9, c10, c11, c12 = st.columns(4)
    c9.metric("52 Week High", details["52 Week High"])
    c10.metric("52 Week Low", details["52 Week Low"])
    c11.metric("Day High", details["Day High"])
    c12.metric("Day Low", details["Day Low"])

    c13, c14, c15, c16 = st.columns(4)
    c13.metric("Volume", details["Volume"])
    c14.metric("Average Volume", details["Avg Volume"])
    c15.metric("Bid", details["Bid"])
    c16.metric("Ask", details["Ask"])

    c17, c18, c19, c20 = st.columns(4)
    c17.metric("Exchange Country", details["Exchange Country"])
    c18.metric("Earnings Date", details["Earnings Date"])
    c19.metric("Ex-Dividend Date", details["Ex-Dividend Date"])
    c20.empty()