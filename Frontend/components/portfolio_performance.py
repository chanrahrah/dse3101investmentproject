from datetime import date
import streamlit as st
import plotly.graph_objects as go
from streamlit_echarts import st_echarts
import math
from streamlit_echarts import st_echarts, JsCode
import pandas as pd
from Backend.backtesting.batch_process_rank_stocks import main
import numpy as np

@st.cache_data
def load_frontend_data(start_date,end_date,initial_capital,topN_stocks,topN_institutions,lag,cost_rate,):
    portfolio_df, metrics_df = main(
        userinput_start_date=str(start_date),
        userinput_end_date=str(end_date),
        userinput_initial_capital=initial_capital,
        userinput_topN_stocks=topN_stocks,
        userinput_topN_institutions=topN_institutions,
        userinput_lag=lag,
        userinput_cost_rate=cost_rate,
    )
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

def count_quarters(portfolio_df):
    return portfolio_df["quarter"].nunique()

def log_returns(series):
    returns = [0]
    for i in range(1, len(series)):
        returns.append(math.log(series[i] / series[i - 1]))
    return returns
    
RF_ANNUAL = 0.0375
RF_QUARTERLY = RF_ANNUAL / 4
# Placeholder for now
def portfolio_performance():
    chart_c1, chart_c2, _ = st.columns([1, 1, 4])
    with chart_c1:
        use_log_scale = st.checkbox("Log scale", value=False)
    with chart_c2:
        show_benchmark = st.checkbox("Show SPY", value=True)

    from_date = st.session_state.get("from_date", None)
    to_date = st.session_state.get("to_date", None)
    initial_capital = st.session_state.get("initial_capital", 10000)
    cost_rate = st.session_state.get("fee_per_trade", 0.001)
    lag = st.session_state.get("lag", 47)
    topN_stocks = st.session_state.get("topN_stocks", 10)
    topN_institutions = st.session_state.get("topN_institutions", 10)

    portfolio_df, metrics_df = load_frontend_data(
        start_date=from_date,
        end_date=to_date,
        initial_capital=initial_capital,
        topN_stocks=topN_stocks,
        topN_institutions=topN_institutions,
        lag=lag,
        cost_rate=cost_rate,
    )
    #portfolio_dates = portfolio_df["quarter"].tolist()
    portfolio_dates = pd.to_datetime(portfolio_df["date"])
    portfolio_values = portfolio_df["portfolio_value"].tolist()
    #spy_values = portfolio_df["spy_value"].tolist()
    spy_values = portfolio_values.copy() # Dummy, until spy_values added in backend
    
    if from_date is None or to_date is None:
        st.warning("Please select date range")
        return
    quarter_end_dates = pd.to_datetime(portfolio_df["date"]).dt.date.tolist()
   
    filtered = [
        (d, label, p, s, t)
        for d, label, p, s, t in zip(
            quarter_end_dates,
            portfolio_dates,
            portfolio_values,
            spy_values,
            portfolio_df["tickers"]
        )
        if from_date <= d <= to_date
    ]

    if not filtered:
        st.warning("No data available for the selected date range")
        return

    _, portfolio_dates, portfolio_values, spy_values, tickers = zip(*filtered)
    portfolio_dates = pd.to_datetime(portfolio_dates)
    portfolio_dates = [d.strftime("%Y-%m-%d") for d in portfolio_dates]
    portfolio_values = list(portfolio_values)
    spy_values = list(spy_values)
    tickers = list(tickers)

    if use_log_scale:
        portfolio_plot = log_returns(portfolio_values)
        spy_plot = log_returns(spy_values)
    else:
        portfolio_plot = portfolio_values
        spy_plot = spy_values

    portfolio_series_data = []
    for val in portfolio_plot:
        point = {
            "value": val,
            "symbolSize": 8,
        }
        portfolio_series_data.append(point)


    if not use_log_scale:
        chart_min = min(portfolio_plot)
        chart_max = max(portfolio_plot)
        if show_benchmark:
            chart_min = min(chart_min, min(spy_plot))
            chart_max = max(chart_max, max(spy_plot))

        padding = (chart_max - chart_min) * 0.1 if chart_max > chart_min else max(chart_max * 0.1, 1)
        y_min = max(0, chart_min - padding)
        y_max = chart_max + padding

    if use_log_scale:
        yAxis = [
            {
                "type": "value",
                "name": "Log Return",
                "position": "left",
                "axisLabel": {
                    "formatter": JsCode(
                        "function(value) { return (value * 100).toFixed(1) + '%'; }"
                    )
                }
            }
        ]
    else:
        yAxis = [
            {
                "type": "value",
                "name": "Portfolio ($)",
                "position": "left",
                "min": y_min,
                "max": y_max,
                "axisLabel": {
                    "formatter": JsCode(
                        "function(value) { return value.toLocaleString(); }"
                    )
                }
            }
        ]

        if show_benchmark:
            yAxis.append(
                {
                    "type": "value",
                    "name": "SPY",
                    "position": "right",
                    "min": y_min,
                    "max": y_max,
                    "axisLabel": {
                        "formatter": JsCode(
                            "function(value) { return value.toLocaleString(); }"
                        )
                    }
                }
            )

    if use_log_scale:
        series = [
            {
                "name": "Portfolio",
                "type": "line",
                "yAxisIndex": 0,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 12,
                "data": portfolio_series_data,
            }
        ]

        if show_benchmark:
            series.append({
                "name": "SPY",
                "type": "line",
                "yAxisIndex": 0,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 7,
                "data": spy_plot,
            })
    else:
        series = [
            {
                "name": "Portfolio",
                "type": "line",
                "yAxisIndex": 0,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 12,
                "data": portfolio_series_data,
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
            })
    legend_data = ["Portfolio"]
    if show_benchmark:
        legend_data.append("SPY")

    chart_option = {
        "title": {
            "text": "Portfolio Performance",
            "left": "center"
        },
        "tooltip": {
            "trigger": "axis",
            "formatter": JsCode(
                f"""
                function (params) {{
                    const idx = params[0].dataIndex;
                    const dates = {portfolio_dates};
                    const tickers = {tickers};

                    let lines = [];
                    lines.push("Date: " + dates[idx]);

                    for (let i = 0; i < params.length; i++) {{
                        let p = params[i];
                        let val = typeof p.value === "number"
                            ? p.value.toLocaleString(undefined, {{ maximumFractionDigits: 2 }})
                            : p.value;
                        lines.push(p.marker + " " + p.seriesName + ": " + val);
                    }}

                    let t = tickers[idx];
                    if (typeof t === "string") {{
                        t = t.replace(/[\[\]'"]/g, "");
                    }}

                    lines.push("Top N Stocks: " + t);

                    return lines.join("<br/>");
                }}
                """
            )
        },
        "legend": {
            "data": legend_data,
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
            "data": portfolio_dates,
            "axisLabel": {
                "interval": "auto",
                "formatter": JsCode(
                    """
                    function (value) {
                        return value.slice(0, 7);
                    }
                    """
                )
            }
        },
        "yAxis": yAxis,
        "dataZoom": [
            {"type": "inside"},
            {"type": "slider"}
        ],
        "series": series
    }

    result = st_echarts(
        chart_option,
        height="450px",
        key=f"portfolio_chart_{use_log_scale}_{show_benchmark}",
        on_select="rerun",
        selection_mode="points",
    )

    #--- Update data based on selected point ---
    selection = result.get("selection", {}) if result else {}
    point_indices = selection.get("point_indices", [])

    if point_indices:
        idx = point_indices[0]
        st.session_state["selected_chart_index"] = idx
        st.session_state["selected_chart_date"] = portfolio_dates[idx]
        st.session_state["selected_chart_tickers"] = tickers[idx]

    if st.session_state.get("selected_chart_date"):
        st.caption(f"Selected point: {st.session_state['selected_chart_date']}")

    starting_capital = portfolio_values[0]
    ending_capital = portfolio_values[-1]

    # CAGR (simple version based on periods)
    number_of_quarters = count_quarters(portfolio_df)
    years = number_of_quarters / 4
    cagr = ((ending_capital / starting_capital) ** (1 / max(years, 1e-6)) - 1) * 100
    
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

    values_with_anchor = [starting_capital] + portfolio_values
    quarterly_returns = pd.Series(values_with_anchor).pct_change().dropna()
    excess = quarterly_returns - RF_QUARTERLY
    sharpe = (excess.mean() / excess.std()) * np.sqrt(4) if excess.std() != 0 else 0
    downside = quarterly_returns[quarterly_returns < RF_QUARTERLY] - RF_QUARTERLY
    downside_std = np.sqrt((downside ** 2).mean()) if len(downside) > 0 else 0
    sortino = (excess.mean() / downside_std) * np.sqrt(4) if downside_std != 0 else 0

    metrics = [
        ("Sharpe Ratio", sharpe, "number"),          # keep placeholder or backend later
        ("Sortino Ratio", sortino, "number"),         # placeholder
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
