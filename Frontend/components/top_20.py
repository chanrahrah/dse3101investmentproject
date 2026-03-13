import streamlit as st
import pandas as pd
import plotly.express as px


# dummy data 
tickers = ["AAPL","MSFT","NVDA","AMZN","META","TSLA","GOOGL","BRK.B","JPM","UNH","V","MA","HD","PG","AVGO","XOM","LLY","COST","ABBV","KO"]
allocation = [10.5,8.0,9.0,8.2,6.0,5.5,5.1,4.8,4.2,3.9,3.5,3.3,3.1,2.9,2.8,2.7,2.6,2.5,2.4,2.3]
recommended = [12.5,11.8,11.5,10.2,9.4,8.6,6.0,5.9,4.3,3.2,2.5,2.2,2.0,1.9,1.8,1.7,1.6,1.5,1.4,1.3]
df = pd.DataFrame({
    "Rank": range(1,21),
    "Ticker": tickers,
    "Allocation": allocation,
    "Recommended allocation": recommended
    })

def top_20_table():

    # table
    edited_df = st.data_editor(
        df,
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "Rank": st.column_config.NumberColumn(disabled=True),
            "Ticker": st.column_config.TextColumn(disabled=True),
            "Allocation": st.column_config.NumberColumn(
                min_value=0.0,
                max_value=100.0,
                step=0.1,
                format="%.1f%%"
            ),
            "Recommended allocation": st.column_config.NumberColumn(
                disabled=True,
                format="%.1f%%"
            ),
            "Signal": st.column_config.TextColumn(
                "Change",
                disabled=True
            )
        },
        hide_index=True
    )