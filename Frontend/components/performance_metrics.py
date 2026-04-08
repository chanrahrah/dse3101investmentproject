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

def get_arrow(portfolio_val, spy_val):
    if portfolio_val is None or spy_val is None:
        return "", "white"

    if portfolio_val > spy_val:
        return "▲", "#015739"  # white
    elif portfolio_val < spy_val:
        return "▼", "#720b00"  # red
    else:
        return "-", "#ffffff"  # white
    
def render_metric(label, value, kind="number", spy_value=None):
    bg = metric_bg(value)

    display_main = format_metric(value, kind)
    display_spy = format_metric(spy_value, kind) if spy_value is not None else "--"
    
    arrow, arrow_color = get_arrow(value, spy_value)

    if label == "Starting Capital":
        arrow = ""

    st.markdown(
        f"""
        <div style="background:{bg}; border-radius:14px; padding:14px; height:110px; color:white;">
        <div style="font-size:16px; font-weight:500; margin-bottom:4px;">{label}</div>
        <div style="display:flex; justify-content:space-between; align-items:center;">
        <div>
                <div style="font-size:12px; opacity:0.8;">Portfolio</div>
                <div style="font-size:28px;">{display_main}
                    <span style="font-size:25px; color:{arrow_color};">{arrow}
                    </span>
                    </div></div>
                <div style="opacity:0.8; padding-top: 20px">|
                </div>
                <div style="text-align:right;">
                <div style="font-size:12px; opacity:0.8;">SPY</div>
                <div style="font-size:28px;">
                    {display_spy}
                </div></div></div></div>
    """, 
    unsafe_allow_html=True)
    
def count_quarters(portfolio_df):
    if "quarter" in portfolio_df.columns:
        return portfolio_df["quarter"].nunique()

    if "date" in portfolio_df.columns:
        dates = pd.to_datetime(portfolio_df["date"], errors="coerce").dropna()
        return dates.dt.to_period("Q").nunique()

    return 0

# Compute metrics # 
def compute_metrics(values, portfolio_df, deduct_fee_from_starting=True):
    if values is None or len(values) < 2:
        return None

    initial_capital = st.session_state.get("initial_capital", 0)
    fee_per_trade = st.session_state.get("fee_per_trade", 0)

    if deduct_fee_from_starting:
        starting = initial_capital - fee_per_trade * initial_capital
    else:
        starting = initial_capital

    ending = values[-1]

    number_of_quarters = count_quarters(portfolio_df)
    years = number_of_quarters / 4 if number_of_quarters > 0 else max(len(values) / 252, 1e-6)

    cagr = ((ending / starting) ** (1 / max(years, 1e-6)) - 1) * 100

    peak = values[0]
    max_dd = 0
    for v in values:
        if v > peak:
            peak = v
        drawdown = (v - peak) / peak
        if drawdown < max_dd:
            max_dd = drawdown
    max_dd *= 100

    profit_to_dd = cagr / abs(max_dd) if max_dd != 0 else None

    rf_daily = 0.0375 / 252
    returns = pd.Series(values).pct_change().dropna()

    sharpe, sortino = 0, 0
    if len(returns) > 0 and returns.std() != 0:
        excess = returns - rf_daily
        sharpe = (excess.mean() / returns.std()) * np.sqrt(252)

        downside = returns[returns < rf_daily] - rf_daily
        downside_std = np.sqrt((downside ** 2).mean()) if len(downside) > 0 else 0
        sortino = (excess.mean() / downside_std) * np.sqrt(252) if downside_std != 0 else 0

    return [
        ("Sharpe Ratio", sharpe, "number"),
        ("Sortino Ratio", sortino, "number"),
        ("CAGR", cagr, "percent"),
        ("Max Drawdown", max_dd, "percent"),
        ("Starting Capital", starting, "currency"),
        ("Ending Capital", ending, "currency"),
        ("Profit / Drawdown", profit_to_dd, "number"),
    ]

def performance_metrics(portfolio_df, metrics_df=None):

    if portfolio_df is None or portfolio_df.empty:
        st.warning("No portfolio data available for metrics.")
        return

    if "portfolio_value" not in portfolio_df.columns:
        st.error("portfolio_df must contain 'portfolio_value'.")
        return

    # ---- values ----
    portfolio_values = portfolio_df["portfolio_value"].tolist()
    spy_values = portfolio_df["spy_value"].tolist()

    # ---- Compute metrics ----
    portfolio_metrics = compute_metrics(portfolio_values, portfolio_df, deduct_fee_from_starting=True)
    spy_metrics = compute_metrics(spy_values, portfolio_df, deduct_fee_from_starting=False) if spy_values else None

    if portfolio_metrics is None:
        st.warning("Not enough data to calculate metrics.")
        return

    row1 = st.columns(4, gap="small")

    for col, p_metric, s_metric in zip(
        row1,
        portfolio_metrics[:4],
        spy_metrics[:4] if spy_metrics else portfolio_metrics[:4]
    ):
        label, p_val, kind = p_metric
        _, s_val, _ = s_metric

        with col:
            render_metric(label, p_val, kind, spy_value=s_val)
            
    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    row2 = st.columns(3, gap="small")

    for col, p_metric, s_metric in zip(
        row2,
        portfolio_metrics[4:],
        spy_metrics[4:] if spy_metrics else portfolio_metrics[4:]
    ):
        label, p_val, kind = p_metric
        _, s_val, _ = s_metric

        with col:
            render_metric(label, p_val, kind, spy_value=s_val)