import pandas as pd
import streamlit as st
from streamlit_echarts import st_echarts, JsCode


def cumulative_returns(portfolio_df: pd.DataFrame):
    if portfolio_df is None or portfolio_df.empty:
        st.warning("No portfolio data available.")
        return

    if "date" not in portfolio_df.columns or "cum_return" not in portfolio_df.columns:
        st.error(
            f"portfolio_df must contain 'date' and 'cum_return'. "
            f"Columns: {list(portfolio_df.columns)}"
        )
        return

    df = portfolio_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["cum_return"] = pd.to_numeric(df["cum_return"], errors="coerce")
    df = df.dropna(subset=["date", "cum_return"]).sort_values("date")

    if df.empty:
        st.warning("No valid data for cumulative returns.")
        return

    dates = df["date"].dt.strftime("%Y-%m-%d").tolist()
    values = (df["cum_return"] * 100).tolist()

    series_data = [{"value": v, "symbolSize": 16} for v in values]

    chart_min = min(values)
    chart_max = max(values)
    padding = (chart_max - chart_min) * 0.1 if chart_max > chart_min else max(abs(chart_max) * 0.1, 1)

    chart_option = {
        "title": {"text": "Cumulative Returns", "left": "center"},
        "tooltip": {"show": False},
        "legend": {"data": ["Cummulative Return"], "top": 40},
        "grid": {"top": 80},
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
            "data": dates,
            "axisLabel": {
                "formatter": JsCode(
                    "function (value) { return value.slice(0, 7); }"
                )
            }
        },
        "yAxis": [{
            "type": "value",
            "name": "Return (%)",
            "min": chart_min - padding,
            "max": chart_max + padding,
            "axisLabel": {
                "formatter": JsCode(
                    "function(value) { return value.toFixed(2) + '%'; }"
                )
            }
        }],
        "dataZoom": [{"type": "inside"}, {"type": "slider"}],
        "series": [{
            "name": "Cumulative Return",
            "type": "line",
            "symbol": "circle",
            "symbolSize": 18,
            "data": series_data,
        }]
    }

    st_echarts(chart_option, height="500px", key="cumulative_returns_chart")