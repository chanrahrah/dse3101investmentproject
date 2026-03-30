from datetime import date
import streamlit as st
import plotly.graph_objects as go
from streamlit_echarts import st_echarts
import math
from streamlit_echarts import st_echarts, JsCode
import pandas as pd

@st.cache_data
def load_frontend_data():

    # assume main() returns:
    # portfolio_df, metrics_df
    portfolio_df, metrics_df = main()
    return portfolio_df, metrics_df

# ---------- metric helper functions ----------
def metric_bg(value):
    if value is None:
        return "#3b3f4a"   # grey
    if value > 0:
        return "#1f9d73"   # green
    if value < 0:
        return "#c63d2f"   # red
    return "#3b3f4a"


def format_metric(value, kind="number"):
    if value is None:
        return "--"
    if kind == "percent":
        return f"{value:.1f}%"
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


def log_returns(series):
    returns = [0]
    for i in range(1, len(series)):
        returns.append(math.log(series[i] / series[i - 1]))
    return returns
    

# Placeholder for now
#st.line_chart([1,2,3,2,5])
def portfolio_performance():
    chart_c1, chart_c2, _ = st.columns([1, 1, 4])
    with chart_c1:
        use_log_scale = st.checkbox("Log scale", value=False)
    with chart_c2:
        show_benchmark = st.checkbox("Show SPY", value=True)

    portfolio_dates = [
        "2022-Q2", "2022-Q3", "2022-Q4",
        "2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4"
    ]

    portfolio_values = [
        8371.937801,
        9314.938979,
        8577.787879,
        8729.342859,
        8768.148067,
        10551.983110,
        10834.367892
    ]

    spy_values = [
        384.624,
        401.411,
        399.312,
        432.685,
        440.866,
        494.167,
        522.047
    ]

    from_date = st.session_state.get("from_date", None)
    to_date = st.session_state.get("to_date", None)

    if from_date is None or to_date is None:
        st.warning("Please select date range")
        return

    quarter_end_dates = [
        date(2022, 6, 30),
        date(2022, 9, 30),
        date(2022, 12, 31),
        date(2023, 3, 31),
        date(2023, 6, 30),
        date(2023, 9, 30),
        date(2023, 12, 31),
    ]

    filtered = [
        (d, label, p, s)
        for d, label, p, s in zip(quarter_end_dates, portfolio_dates, portfolio_values, spy_values)
        if from_date <= d <= to_date
    ]

    if not filtered:
        st.warning("No data available for the selected date range")
        # fallback to full dataset
        filtered = list(zip(quarter_end_dates, portfolio_dates, portfolio_values, spy_values))

    _, portfolio_dates, portfolio_values, spy_values = zip(*filtered)
    portfolio_dates = list(portfolio_dates)
    portfolio_values = list(portfolio_values)
    spy_values = list(spy_values)

    if use_log_scale:
        portfolio_plot = log_returns(portfolio_values)
        spy_plot = log_returns(spy_values)
    else:
        portfolio_plot = portfolio_values
        spy_plot = spy_values

    # To integrate with backend, replace portfolio_dates and portfolio_values with backend output:
    # portfolio_dates = backend_output["dates"]
    # portfolio_values = backend_output["portfolio_values"]
    # spy_values = backend_output["spy_values"]

    if use_log_scale:
        yAxis = {
            "type": "value",
            "axisLabel": {
                "formatter": JsCode(
                    "function(value) { return (value * 100).toFixed(1) + '%'; }"
                )
            }
        }
    else:
        yAxis = [
            {
                "type": "value",
                "name": "Portfolio ($)",
                "position": "left",
                "axisLabel": {
                    "formatter": JsCode(
                        "function(value) { return value.toLocaleString(); }"
                    )
                }
            },
            {
                "type": "value",
                "name": "SPY",
                "position": "right",
                "axisLabel": {
                    "formatter": JsCode(
                        "function(value) { return value.toLocaleString(); }"
                    )
                }
            }
        ]
    #portfolio_area = {"opacity": 0.22}
    #spy_area = {"opacity": 0.42}

    if use_log_scale:
        series = [
            {
                "name": "Portfolio",
                "type": "line",
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 8,
                "data": portfolio_plot,
                #"areaStyle": {"opacity": 0.22}
            }
        ]

        if show_benchmark:
            series.append({
                "name": "SPY",
                "type": "line",
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 7,
                "data": spy_plot,
                #"areaStyle": {"opacity": 0.42}
            })
    else:
        series = [
            {
                "name": "Portfolio",
                "type": "line",
                "yAxisIndex": 0,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 8,
                "data": portfolio_plot,
                #"areaStyle": {"opacity": 0.22}
            }
        ]

        if show_benchmark:
            series.append({
                "name": "SPY",
                "type": "line",
                "yAxisIndex": 1,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 7,
                "data": spy_plot,
                #"areaStyle": {"opacity": 0.42}
            })

    chart_option = {
        "title": {
            "text": "Portfolio Performance",
            "left": "center"
        },
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": ["Portfolio", "SPY"],
            "top": 40
        },
        "grid": {
            "top": 80
        },
        "toolbox": {
            "feature": {
                "saveAsImage": {},
                "dataView": {"readOnly": True},
                "restore": {},
                "dataZoom": {}
            }
        },
        "xAxis": {
            "type": "category",
            "boundaryGap": False,
            "data": portfolio_dates
        },
        "yAxis": yAxis,
        "dataZoom": [
            {"type": "inside"},
            {"type": "slider"}
        ],
        "series": series
    }

    st_echarts(chart_option, height="450px")

    starting_capital = portfolio_values[0]
    ending_capital = portfolio_values[-1]

    # CAGR (simple version based on periods)
    num_periods = len(portfolio_values) - 1
    cagr = ((ending_capital / starting_capital) ** (1 / max(num_periods, 1)) - 1) * 100

    # Max Drawdown
    peak = portfolio_values[0]
    max_drawdown = 0
    for v in portfolio_values:
        if v > peak:
            peak = v
        drawdown = (v - peak) / peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    max_drawdown *= 100  # convert to %

    # Profit to Drawdown ratio
    profit_to_dd = None
    if max_drawdown != 0:
        profit_to_dd = cagr / abs(max_drawdown)

    metrics = [
        ("Sharpe Ratio", 0.40, "number"),          # keep placeholder or backend later
        ("Sortino Ratio", None, "number"),         # placeholder
        ("CAGR", cagr, "percent"),
        ("Max Drawdown", max_drawdown, "percent"),
        ("Starting Capital", starting_capital, "number"),
        ("Ending Capital", ending_capital, "number"),
        ("Profit / Drawdown", profit_to_dd, "number"),
    ]

    metric_row_1 = st.columns(4,gap="small")

    for col, metric in zip(metric_row_1, metrics[:4]):
        with col:
            render_metric(*metric)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    metric_row_2 = st.columns(3,gap="small")

    for col, metric in zip(metric_row_2, metrics[4:]):
        with col:
            render_metric(*metric)