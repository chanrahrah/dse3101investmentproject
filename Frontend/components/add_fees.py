import streamlit as st
import pandas as pd

# create button
def add_fees():
    # Connect this to backend or portfolio calculation so the entered fee per trade affects portfolio results and metrics.
    fee_per_trade = st.number_input(
        "Fees per trade ($)",
        min_value=0.0,
        value=0.0,
        step=0.1,
        key="fee_per_trade"
    )

    return fee_per_trade