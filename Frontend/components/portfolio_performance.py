from datetime import date
import streamlit as st
import plotly.graph_objects as go
from streamlit_echarts import st_echarts
import math
from streamlit_echarts import st_echarts, JsCode
import pandas as pd
from Backend.backtesting.batch_process_rank_stocks import main
import numpy as np

#---------- Function to load data from backend ----------
@st.cache_data
def load_frontend_data(start_date, end_date, initial_capital, topN_stocks, topM_institutions, cost_rate):
    portfolio_df, metrics_df = main(
        userinput_start_date=str(start_date),
        userinput_end_date=str(end_date),
        userinput_initial_capital=initial_capital,
        userinput_topM_institutions=topM_institutions,
        userinput_topN_stocks=topN_stocks,
        userinput_cost_rate=cost_rate,
    )
    return portfolio_df, metrics_df

# ---------- helper functions ----------
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

#--------- Main function to render portfolio performance chart and metrics ----------
def portfolio_performance(portfolio_df, metrics_df):
    chart_c1, chart_c2, _ = st.columns([1, 1, 4])
    with chart_c1:
        use_log_scale = st.checkbox("Log scale", value=False)
    with chart_c2:
        show_benchmark = st.checkbox("Show SPY", value=True)

    from_date = st.session_state.get("from_date", None)
    to_date = st.session_state.get("to_date", None)

    portfolio_dates = pd.to_datetime(portfolio_df["date"])
    portfolio_values = portfolio_df["portfolio_value"].tolist()
    #spy_values = portfolio_df["spy_value"].tolist()
    spy_values = portfolio_values.copy() # Dummy, until spy_values added in backend
    
    if from_date is None or to_date is None:
        st.warning("Please select date range")
        return
    quarter_end_dates = pd.to_datetime(portfolio_df["date"]).dt.date.tolist()
   
    filtered = [
        (d, label, p, s, t, hp)
        for d, label, p, s, t, hp in zip(
            quarter_end_dates,
            portfolio_dates,
            portfolio_values,
            spy_values,
            portfolio_df["tickers"],
            portfolio_df["holding_period"]
        )
        if from_date <= d <= to_date
    ]

    if not filtered:
        st.warning("No data available for the selected date range")
        return

    _, portfolio_dates, portfolio_values, spy_values, tickers, holding_periods = zip(*filtered)
    portfolio_dates = pd.to_datetime(portfolio_dates)
    portfolio_dates = [d.strftime("%Y-%m-%d") for d in portfolio_dates]
    portfolio_values = list(portfolio_values)
    spy_values = list(spy_values)
    tickers = list(tickers)
    holding_periods = list(holding_periods)

    trade_lines = []
    seen_buy_dates = set()
    seen_sell_dates = set()

    for hp in holding_periods:
        if not hp or " to " not in str(hp):
            continue

        buy_date, sell_date = [x.strip() for x in str(hp).split(" to ", 1)]

        if buy_date and buy_date not in seen_buy_dates:
            seen_buy_dates.add(buy_date)
            trade_lines.append({
                "xAxis": buy_date,
                "lineStyle": {
                    "type": "dashed",
                    "width": 1.5,
                    "opacity": 0.8,
                    "color": "#22c55e"   # green = buy
                },
                "label": {
                    "show": False
                }
            })

        if sell_date and sell_date not in seen_sell_dates:
            seen_sell_dates.add(sell_date)
            trade_lines.append({
                "xAxis": sell_date,
                "lineStyle": {
                    "type": "dashed",
                    "width": 1.5,
                    "opacity": 0.8,
                    "color": "#ef4444"   # red = sell
                },
                "label": {
                    "show": False
                }
            })

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
            "symbolSize": 16,
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
                "symbolSize": 18,
                "selectedMode": "single",
                "select": {
                    "itemStyle": {
                        "color": "#f59e0b",
                        "borderColor": "#ffffff",
                        "borderWidth": 2
                    }
                },
                "data": portfolio_series_data,
                "markLine": {
                    "symbol": ["none", "none"],
                    "silent": True,
                    "data": trade_lines
                }
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
                "symbolSize": 18,
                "selectedMode": "single",
                "select": {
                    "itemStyle": {
                        "color": "#f59e0b",
                        "borderColor": "#ffffff",
                        "borderWidth": 2
                    }
                },
                "data": portfolio_series_data,
                "markLine": {
                    "symbol": ["none", "none"],
                    "silent": True,
                    "data": trade_lines
                }
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
            "show": False
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
        "markLine": {
            "symbol": ["none", "none"],
            "silent": True,
            "data": trade_lines
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

    if "selected_chart_index" not in st.session_state:
        st.session_state["selected_chart_index"] = None
    if "selected_chart_date" not in st.session_state:
        st.session_state["selected_chart_date"] = None
    if "selected_chart_tickers" not in st.session_state:
        st.session_state["selected_chart_tickers"] = None

    result = st_echarts(
        chart_option,
        height="450px",
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

    if st.session_state.get("selected_chart_date"):
        st.caption(f"Selected point: {st.session_state['selected_chart_date']}")

    # DEBUG
    #st.write("Stored index:", st.session_state.get("selected_chart_index"))
    #st.write("Stored tickers:", st.session_state.get("selected_chart_tickers"))