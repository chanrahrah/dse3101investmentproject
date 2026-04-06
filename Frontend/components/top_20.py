import streamlit as st
import pandas as pd


def format_value(value, value_type="text"):
    try:
        if pd.isna(value):
            return "N/A"
    except (TypeError, ValueError):
        pass

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

    if "ticker" not in stock_snapshot_df.columns:
        return default_details

    stock_row = stock_snapshot_df[
        stock_snapshot_df["ticker"].astype(str).str.strip().str.upper()
        == str(selected_ticker).strip().upper()
    ]

    if stock_row.empty:
        return default_details

    stock_row = stock_row.iloc[0]

    return {
        "Market Cap": format_value(stock_row["market_cap"], "market_cap"),
        "PE Ratio": format_value(stock_row["pe_ratio"], "ratio"),
        "EPS": format_value(stock_row["eps"], "eps"),
        "Beta": format_value(stock_row["beta"], "beta"),
        "Forward Dividend Yield": format_value(stock_row["forward_dividend_yield"], "percent"),
        "Current Price": format_value(stock_row["close"], "price"),
        "Previous Close": format_value(stock_row["previous_close"], "price"),
        "1Y Target Est": format_value(stock_row["one_year_target_est"], "target"),
        "52 Week High": format_value(stock_row["fifty_two_week_high"], "price"),
        "52 Week Low": format_value(stock_row["fifty_two_week_low"], "price"),
        "Day High": format_value(stock_row["day_high"], "price"),
        "Day Low": format_value(stock_row["day_low"], "price"),
        "Volume": format_value(stock_row["volume"], "volume"),
        "Avg Volume": format_value(stock_row["avg_volume"], "volume"),
        "Bid": format_value(stock_row["bid"], "price"),
        "Ask": format_value(stock_row["ask"], "price"),
        "Exchange Country": format_value(stock_row["exchange_country"]),
        "Earnings Date": format_value(stock_row["earnings_date"], "date"),
        "Ex-Dividend Date": format_value(stock_row["ex_dividend_date"], "date"),
    }

def top_20_table(portfolio_df, top_n=10, top_m_institutions=10, fee_per_dollar=None):
    if portfolio_df is None or portfolio_df.empty:
        st.info("No holdings data available.")
        return None

    clicked_date = st.session_state.get("selected_chart_date")
    clicked_tickers = st.session_state.get("selected_chart_tickers")

    # use exact clicked tickers from chart
    if isinstance(clicked_tickers, list) and len(clicked_tickers) > 0:
        tickers = clicked_tickers
        selected_date_display = clicked_date
    else:
        df = portfolio_df.copy()
        default_to_date = st.session_state.get("to_date")

        if "quarter" in df.columns:
            df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
            df = df.dropna(subset=["quarter"])
            df["quarter_str"] = df["quarter"].dt.strftime("%Y-%m-%d")

            if default_to_date is not None:
                default_to_date_str = pd.to_datetime(default_to_date).strftime("%Y-%m-%d")
                matched = df[df["quarter_str"] == default_to_date_str]
                if not matched.empty:
                    selected_row = matched.iloc[0]
                else:
                    selected_row = df.sort_values("quarter").iloc[-1]
            else:
                selected_row = df.sort_values("quarter").iloc[-1]

            selected_date_display = selected_row["quarter"].strftime("%Y-%m-%d")

        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

            if default_to_date is not None:
                default_to_date_str = pd.to_datetime(default_to_date).strftime("%Y-%m-%d")
                matched = df[df["date_str"] == default_to_date_str]
                if not matched.empty:
                    selected_row = matched.iloc[0]
                else:
                    selected_row = df.sort_values("date").iloc[-1]
            else:
                selected_row = df.sort_values("date").iloc[-1]

            selected_date_display = selected_row["date"].strftime("%Y-%m-%d")

        else:
            st.info("No date information available.")
            return None

        tickers = selected_row["tickers"]

        if "quarter" in df.columns:
            df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
            df = df.dropna(subset=["quarter"])
            selected_row = df.sort_values("quarter").iloc[-1]
            selected_date_display = selected_row["quarter"].strftime("%Y-%m-%d")
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            selected_row = df.sort_values("date").iloc[-1]
            selected_date_display = selected_row["date"].strftime("%Y-%m-%d")
        else:
            st.info("No date information available.")
            return None

        tickers = selected_row["tickers"]

    if not isinstance(tickers, list) or len(tickers) == 0:
        st.info("No tickers available for this period.")
        return None

    tickers = tickers[:top_n]

    st.header(f"Top {top_n} Stocks based on Top {top_m_institutions} Institution Holdings", 
                 help = "Click on any data point in the Portfolio Performance chart to see the top stocks for that quarter.")
    st.caption(f"Selected quarter: {selected_date_display}")
    st.caption(f"Fees per dollar value of transaction ($): {fee_per_dollar}")

    display_df = pd.DataFrame({
        "Rank": range(1, len(tickers) + 1),
        "Ticker": tickers
    })

    row_height = 35
    header_height = 36
    max_visible_rows = 20
    visible_rows = min(len(display_df), max_visible_rows)
    table_height = header_height + visible_rows * row_height - 8

    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        height=table_height,
        column_config={
            "Rank": st.column_config.NumberColumn("Rank", width="small"),
            "Ticker": st.column_config.TextColumn("Ticker", width="medium"),
        }
    )

    return tickers


def render_stock_details(tickers, stock_snapshot_df):
    """
    tickers: list of ticker strings returned by top_20_table()
    stock_snapshot_df: the loaded parquet DataFrame
    """
    if not tickers:
        st.info("Select a stock to view more details.")
        return

    # Let the user pick one ticker from the list
    selected_ticker = st.selectbox(
        "Select a stock to view more details:",
        options=tickers,
        key="selected_ticker_details",
    )

    details = get_stock_details(selected_ticker, stock_snapshot_df)

    st.subheader(f"{selected_ticker} Details")
    st.caption("Stock information accurate as of 2 April 2026 (US market close)")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Market Cap", details["Market Cap"])
    c2.metric("PE Ratio", details["PE Ratio"])
    c3.metric("Earnings Per Share", details["EPS"])
    c4.metric("Beta", details["Beta"])
    c5.metric("Forward Dividend Yield", details["Forward Dividend Yield"])

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Current Price", details["Current Price"])
    c7.metric("Previous Close", details["Previous Close"])
    c8.metric("1Y Target Est", details["1Y Target Est"])
    c9.metric("52 Week High", details["52 Week High"])
    c10.metric("52 Week Low", details["52 Week Low"])

    c11, c12, c13, c14, c15 = st.columns(5)
    c11.metric("Day High", details["Day High"])
    c12.metric("Day Low", details["Day Low"])
    c13.metric("Volume", details["Volume"])
    c14.metric("Avg Volume", details["Avg Volume"])
    c15.metric("Exchange Country", details["Exchange Country"])

    c16, c17, c18, c19, c20 = st.columns(5)
    c16.metric("Bid", details["Bid"])
    c17.metric("Ask", details["Ask"])
    c18.metric("Earnings Date", details["Earnings Date"])
    c19.metric("Ex-Dividend Date", details["Ex-Dividend Date"])
    c20.empty()