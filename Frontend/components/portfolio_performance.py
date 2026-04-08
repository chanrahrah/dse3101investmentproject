from datetime import date
import streamlit as st
import plotly.graph_objects as go
from streamlit_echarts import st_echarts
import math
from streamlit_echarts import st_echarts, JsCode
import pandas as pd
from Backend.backtesting.batch_process_rank_stocks import main
import numpy as np
import json

#---------- Function to load data from backend ----------
@st.cache_data
# ---------- helper functions ----------
def count_quarters(portfolio_df):
    return portfolio_df["quarter"].nunique()

def log_returns(series):
    returns = [0]
    for i in range(1, len(series)):
        log_ret = math.log(series[i] / series[i - 1])
        returns.append(returns[-1] + log_ret)  # cumulative sum
    return returns

#--------- Main function to render portfolio performance chart and metrics ----------
def portfolio_performance(portfolio_df: pd.DataFrame):
    chart_c1, chart_c2, chart_c3, chart_c4 = st.columns([4, 1, 1, 4])
    with chart_c2:
        use_log_scale = st.checkbox("Log scale", value=False)
    with chart_c3:
        show_benchmark = st.checkbox("Show SPY", value=True)

    from_date = st.session_state.get("from_date", None)
    to_date = st.session_state.get("to_date", None)

    portfolio_dates = pd.to_datetime(portfolio_df["date"])
    portfolio_values = portfolio_df["portfolio_value"].tolist()
    if "spy_value" in portfolio_df.columns:
        spy_values = portfolio_df["spy_value"].tolist()
    else:
        spy_values = portfolio_values.copy()
    
    if from_date is None or to_date is None:
        st.warning("Please select date range.")
        return
    quarter_end_dates = pd.to_datetime(portfolio_df["date"]).dt.date.tolist()
   
    filtered = [
        (d, label, p, s, t, hp,td)
        for d, label, p, s, t, hp, td in zip(
            quarter_end_dates,
            portfolio_dates,
            portfolio_values,
            spy_values,
            portfolio_df["tickers"],
            portfolio_df["holding_period"],
            portfolio_df["trade_date"]
        )
        if from_date <= d <= to_date
    ]
    #----- DEBUG TO SHOW SELECTED DATES -----
    #st.write("Requested range:", from_date, "to", to_date)
    #st.write("First filtered date:", filtered[0][0] if filtered else None)
    #st.write("Last filtered date:", filtered[-1][0] if filtered else None)

    if not filtered:
        st.warning("No data available for the selected date range")
        return

    _, portfolio_dates, portfolio_values, spy_values, tickers, holding_periods, trade_dates = zip(*filtered)
    portfolio_dates = pd.to_datetime(portfolio_dates)
    portfolio_dates = [d.strftime("%Y-%m-%d") for d in portfolio_dates]
    portfolio_values = list(portfolio_values)
    spy_values = list(spy_values)
    tickers = list(tickers)
    holding_periods = list(holding_periods)
    trade_dates = list(trade_dates)

    portfolio_dates_dt = pd.to_datetime(portfolio_dates)

    trade_dates_dt = pd.to_datetime(trade_dates, errors="coerce")
    show_trade = (portfolio_dates_dt == trade_dates_dt).tolist()

    show_label = [False] * len(portfolio_dates_dt)

    for i, d in enumerate(portfolio_dates_dt):
        is_first = (i == 0)
        is_last = (i == len(portfolio_dates_dt) - 1)

        month = d.month
        is_quarter_month = month in [3, 6, 9, 12]

        is_last_of_month = (
            i == len(portfolio_dates_dt) - 1
            or portfolio_dates_dt[i + 1].month != d.month
            or portfolio_dates_dt[i + 1].year != d.year
        )

        if is_first or is_last or (is_quarter_month and is_last_of_month):
            show_label[i] = True

    show_label_js = json.dumps(show_label)

    if use_log_scale:
        portfolio_plot = log_returns(portfolio_values)
        spy_plot = log_returns(spy_values)
    else:
        portfolio_plot = portfolio_values
        spy_plot = spy_values

    portfolio_series_data = []
    for val, show in zip(portfolio_plot, show_trade):
        point = {
            "value": val,
            "symbolSize": 10 if show else 0,
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
                "symbolSize": 10,
                "selectedMode": "single",
                "select": {
                    "itemStyle": {
                        "color": "#f59e0b",
                        "borderColor": "#ffffff",
                        "borderWidth": 0
                    }
                },
                "data": portfolio_series_data
            }
        ]

        if show_benchmark:
            series.append({
                "name": "SPY",
                "type": "line",
                "yAxisIndex": 0,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 0,
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
                "symbolSize": 16,
                "selectedMode": "single",
                "select": {
                    "itemStyle": {
                        "color": "#f59e0b",
                        "borderColor": "#ffffff",
                        "borderWidth": 2
                    }
                },
                "data": portfolio_series_data
            }
        ]

        if show_benchmark:
            series.append({
                "name": "SPY",
                "type": "line",
                "yAxisIndex": 1,
                "smooth": False,
                "symbol": "circle",
                "symbolSize": 0,
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
            "show": True,
            "trigger": "axis",
            "axisPointer": {
                "type": "cross"
            },
            "formatter": JsCode(
                """
                function (params) {
                    if (!params || params.length === 0) return '';

                    const date = params[0].axisValue;
                    let text = date + '<br/>';

                    params.forEach(function(item) {
                        let val = item.data;

                        if (val && typeof val === 'object' && val.value !== undefined) {
                            val = val.value;
                        }

                        if (typeof val === 'number') {
                            text += item.marker + ' ' + item.seriesName + ': ' + val.toLocaleString() + '<br/>';
                        }
                    });

                    return text;
                }
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
                "interval": 0,
                "showMinLabel": True,
                "showMaxLabel": True,
                "hideOverlap": True,
                "fontSize": 10,
                "formatter": JsCode(
                    f"""
                    function (value, index) {{
                        const showLabel = {show_label_js};
                        return showLabel[index] ? value.slice(0, 7) : "";
                    }}
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

    if "selected_chart_index" not in st.session_state:
        st.session_state["selected_chart_index"] = None
    if "selected_chart_date" not in st.session_state:
        st.session_state["selected_chart_date"] = None
    if "selected_chart_tickers" not in st.session_state:
        st.session_state["selected_chart_tickers"] = None

    result = st_echarts(
        chart_option,
        height="500px",
        width="100%",
        key="portfolio_chart",
        on_select="rerun",
        selection_mode="points",
    )

    if result and isinstance(result, dict):
        selection = result.get("selection", {})
        point_indices = selection.get("point_indices", [])

        if point_indices:
            idx = point_indices[0]
            if 0 <= idx < len(portfolio_dates):
                st.session_state["selected_chart_index"] = idx
                st.session_state["selected_chart_date"] = portfolio_dates[idx]
                st.session_state["selected_chart_tickers"] = tickers[idx]

    # DEBUG
    #st.write("Stored index:", st.session_state.get("selected_chart_index"))
    #st.write("Stored tickers:", st.session_state.get("selected_chart_tickers"))