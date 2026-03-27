import streamlit as st
import plotly.graph_objects as go
from streamlit_echarts import st_echarts
import math
from streamlit_echarts import st_echarts, JsCode

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
    portfolio_area = {"opacity": 0.22}
    spy_area = {"opacity": 0.42}

    if use_log_scale:
        series = [
            {
                "name": "Portfolio",
                "type": "line",
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 8,
                "data": portfolio_plot,
                "areaStyle": {"opacity": 0.22}
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
                "areaStyle": {"opacity": 0.42}
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
                "areaStyle": {"opacity": 0.22}
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
                "areaStyle": {"opacity": 0.42}
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

    metrics = [
        ("CAGR", 6.37, "percent"),
        ("Sharpe Ratio", 0.40, "number"),
        ("Max Drawdown", -21.88, "percent"),
        ("Volatility", 21.04, "percent"),
        ("Total Return", 8.34, "percent"),
        ("Alpha", None, "percent"),
        ("Beta", None, "number"),
        ("Win percentage", None, "percent"),
    ]
    ###To integrate with backend, replace metrics with code below and replacewith output from backend:
    ###metrics = [
    ###    ("CAGR", backend_output["cagr"], "percent"),
    ###    ("Sharpe Ratio", backend_output["sharpe_ratio"], "number"),
    ###    ("Max Drawdown", backend_output["max_drawdown"], "percent"),
    ###    ("Volatility", backend_output["volatility"], "percent"),
    ###    ("Total Return", backend_output["total_return"], "percent"),
    ###    ("Alpha", backend_output["alpha"], "percent"),
    ###    ("Beta", backend_output["beta"], "number"),
    ###    ("Win percentage", backend_output["win_percentage"], "percent"),
    ###]

    metric_row_1 = st.columns(4,gap="small")
    metric_row_2 = st.columns(4,gap="small")

    for col, metric in zip(metric_row_1, metrics[:4]):
        with col:
            render_metric(*metric)

    for col, metric in zip(metric_row_2, metrics[4:]):
        with col:
            render_metric(*metric)