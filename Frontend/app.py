import streamlit as st
import plotly.graph_objects as go
from streamlit_echarts import st_echarts
from components.new_backtest import new_backtest_button #for yenfay's code testing, DO NOT OVERWRITE PLEASE! 
from components.top_20 import top_20_table #for yenfay's code testing, DO NOT OVERWRITE PLEASE!
from datetime import date 

# page set up and layout
st.set_page_config(
    page_title="dse3101 project",
    layout="wide"
)

# title and new backtest button layout
c_title, c_backtest = st.columns([9, 1], vertical_alignment = "center")
with c_title: 
    st.title("Beginner Dashboard")
    
# date layout
c1, c2, c3 = st.columns([0.8, 0.1, 0.1])
quarter_end_dates = [
    date(2025, 3, 31),
    date(2025, 6, 30),
    date(2025, 9, 30),
    date(2025, 12, 31),
]

with c1:
    st.write("")

with c2:
    from_date = st.selectbox(
        "From:",
        options=quarter_end_dates,
        index=0,
        format_func=lambda d: d.strftime("%Y/%m/%d"),
        key="from_date"
    )

with c3:
    valid_to_dates = [d for d in quarter_end_dates if d >= from_date]

    to_date = st.selectbox(
        "To:",
        options=valid_to_dates,
        index=len(valid_to_dates) - 1,
        format_func=lambda d: d.strftime("%Y/%m/%d"),
        key="to_date"
    )

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
    
# configure left column for graph and right column for table
col_left, col_right = st.columns([6, 4])

with col_left:
    st.header("Porfolio performance")
    ### ruiqian add your code here for the metrics ###
    # Placeholder for now
    #st.line_chart([1,2,3,2,5])
    portfolio_dates = [
        "2025-01", "2025-02", "2025-03", "2025-04",
        "2025-05", "2025-06", "2025-07", "2025-08",
        "2025-09", "2025-10", "2025-11", "2025-12"
    ]

    portfolio_values = [
        100000, 125000, 118000, 140000,
        155000, 210000, 175000, 235000,
        248000, 225000, 275000, 260000
    ]
    
    # To integrate with backend, replace portfolio_dates and portfolio_values with code below and replace with output from backend:
    #portfolio_dates = backend_output["dates"]
    #portfolio_values = backend_output["portfolio_values"]

    chart_option = {
        "title": {
            "text": "Portfolio Performance",
            "left": "center"
        },
        "tooltip": {
            "trigger": "axis"
        },
        "toolbox": {
            "feature": {
                "saveAsImage": {},
                "dataView": {"readOnly": True},
                "restore": {},
                "dataZoom": {},
            }
        },
        "xAxis": {
            "type": "category",
            "boundaryGap": False,
            "data": portfolio_dates
        },
        "yAxis": {
            "type": "value"
        },
        "dataZoom": [
            {"type": "inside"},
            {"type": "slider"}
        ],
        "series": [
            {
                "name": "Portfolio",
                "type": "line",
                "smooth": True,
                "symbol": "circle",
                "symbolSize": 8,
                "data": portfolio_values,
                "areaStyle": {}
            }
        ]
    }

    st_echarts(chart_option, height="450px")

    metrics = [
        ("CAGR", 46.7, "percent"),
        ("Sharpe Ratio", 1.13, "number"),
        ("Max Drawdown", -12.3, "percent"),
        ("Volatility", 17.8, "percent"),
        ("Total Return", None, "percent"),
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

with col_right:
    st.header("Top 10 Stocks by Institutional Holdings")
    top_20_table()