import streamlit as st
import pandas as pd
import numpy as np


def metric_bg(value):
    if value is None:
        return "#3b3f4a"
    if value > 0:
        return "#1f9d73"
    if value < 0:
        return "#c63d2f"
    return "#3b3f4a"


def format_metric(value, kind="number"):
    if value is None:
        return "--"
    if kind == "percent":
        return f"{value:.1f}%"
    if kind == "currency":
        return f"{value:,.2f}"
    return f"{value:.2f}"


def render_metric(label, value, kind="number"):
    bg = metric_bg(value)
    display = format_metric(value, kind)

    st.markdown(
        f"""
        <div style="
            background:{bg};
            border-radius:14px;
            padding:14px;
            height:80px;
            display:flex;
            flex-direction:column;
            justify-content:space-between;
            color:white;
        ">
            <div style="font-size:16px;font-weight:600;">{label}</div>
            <div style="font-size:16px;font-weight:700;">{display}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def count_quarters(portfolio_df):
    if "quarter" in portfolio_df.columns:
        return portfolio_df["quarter"].nunique()

    if "date" in portfolio_df.columns:
        dates = pd.to_datetime(portfolio_df["date"], errors="coerce").dropna()
        return dates.dt.to_period("Q").nunique()

    return 0


def performance_metrics(portfolio_df, metrics_df=None):
    if portfolio_df is None or portfolio_df.empty:
        st.warning("No portfolio data available for metrics.")
        return

    if "portfolio_value" not in portfolio_df.columns:
        st.error("portfolio_df must contain 'portfolio_value'.")
        return

    portfolio_values = pd.to_numeric(
        portfolio_df["portfolio_value"], errors="coerce"
    ).dropna().tolist()

    if len(portfolio_values) < 2:
        st.warning("Not enough data to calculate metrics.")
        return

    starting_capital = portfolio_values[0]
    ending_capital = portfolio_values[-1]

    number_of_quarters = count_quarters(portfolio_df)
    years = number_of_quarters / 4 if number_of_quarters > 0 else max(len(portfolio_values) / 252, 1e-6)
    cagr = ((ending_capital / starting_capital) ** (1 / max(years, 1e-6)) - 1) * 100

    peak = portfolio_values[0]
    max_drawdown = 0
    for v in portfolio_values:
        if v > peak:
            peak = v
        drawdown = (v - peak) / peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    max_drawdown *= 100

    profit_to_dd = None
    if max_drawdown != 0:
        profit_to_dd = cagr / abs(max_drawdown)

    rf_annual = 0.0375
    rf_daily = rf_annual / 252
    daily_returns = pd.Series(portfolio_values).pct_change().dropna()

    sharpe = 0
    sortino = 0

    if len(daily_returns) > 0 and daily_returns.std() != 0:
        excess = daily_returns - rf_daily
        sharpe = (excess.mean() / daily_returns.std()) * np.sqrt(252)

        downside = daily_returns[daily_returns < rf_daily] - rf_daily
        downside_std = np.sqrt((downside ** 2).mean()) if len(downside) > 0 else 0
        sortino = (excess.mean() / downside_std) * np.sqrt(252) if downside_std != 0 else 0

    metrics = [
        ("Sharpe Ratio", sharpe, "number"),
        ("Sortino Ratio", sortino, "number"),
        ("CAGR", cagr, "percent"),
        ("Max Drawdown", max_drawdown, "percent"),
        ("Starting Capital", starting_capital, "currency"),
        ("Ending Capital", ending_capital, "currency"),
        ("Profit / Drawdown", profit_to_dd, "number"),
    ]

    metric_row_1 = st.columns(4, gap="small")
    for col, metric in zip(metric_row_1, metrics[:4]):
        with col:
            render_metric(*metric)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    metric_row_2 = st.columns(3, gap="small")
    for col, metric in zip(metric_row_2, metrics[4:]):
        with col:
            render_metric(*metric)