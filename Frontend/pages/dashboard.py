import streamlit as st
import pandas as pd
import numpy as np

# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(layout="wide")
st.title("Top Institutions")
st.markdown("""
<style>
.stApp {
    background-color: black;
    color: #ccc;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# Dummy data
# -----------------------------
data = pd.DataFrame({
    "Ticker": ["AAPL", "NVDA", "MSFT", "AMZN", "TSLA", "GOOG", "META"],
    "Stock": ["Apple Inc.", "Nvidia Corp", "Microsoft Corp", "Amazon.com Inc.", "Tesla Inc.", "Alphabet Cl C", "Meta Platforms"],
    "Shares Held": [12500000, 8400000, 10200000, 9100000, 5000000, 7200000, 6800000],
    "Market Value": [2800000000, 1950000000, 3100000000, 1600000000, 850000000, 1100000000, 1400000000],
    "% of Portfolio": [15.2, 12.5, 14.8, 10.1, 5.4, 7.2, 8.9],
    "Change in Shares": np.random.randint(10000, 100000, 7),
    "% Change": np.random.uniform(1, 5, 7)
})

# -----------------------------
# Top 5 increases
# -----------------------------
top_increases = data.sort_values("% Change", ascending=False).head(5).copy()
top_increases.insert(0, "Rank", range(1, len(top_increases)+1))
top_increases['Change in Shares'] = (top_increases['Change in Shares'] / 1e6).round(2).astype(str) + "M"
top_increases['% Change'] = top_increases['% Change'].round(2).apply(lambda x: f"▲ {x}%")

# -----------------------------
# Top Increases Table
# -----------------------------
display_df = top_increases[['Rank', 'Ticker', 'Change in Shares', '% Change']].copy()
display_df.index = range(len(display_df))

left_col, right_col = st.columns([1, 1])

with left_col:
    
    with st.container(border=True):
        st.markdown("<h3 style='margin:0; color: #2e7d32;'>Top Increases</h3>", unsafe_allow_html=True)
        
        h1, h2, h3, h4 = st.columns([1, 1, 1, 1])
        h1.markdown("<p style='color:white; font-weight:bold; text-align:center; margin:0;'>Rank</p>", unsafe_allow_html=True)
        h2.markdown("<p style='color:white; font-weight:bold; text-align:center; margin:0;'>Ticker</p>", unsafe_allow_html=True)
        h3.markdown("<p style='color:white; font-weight:bold; text-align:center; margin:0;'>Change</p>", unsafe_allow_html=True)
        h4.markdown("<p style='color:white; font-weight:bold; text-align:center; margin:0;'>% Change</p>", unsafe_allow_html=True)
        
        st.markdown("<hr style='margin:10px 0; border:0.5px solid #333;'>", unsafe_allow_html=True)

        for _, row in top_increases.iterrows():
            r1, r2, r3, r4 = st.columns([1, 1, 1, 1])
            raw_shares = str(row["Change in Shares"]).replace('M', '')
            formatted_shares = f"+{float(raw_shares):.2f}M"
            raw_pct = str(row["% Change"]).replace("▲ ", "").replace("%", "").replace("+", "")
            formatted_pct = f"▲ +{float(raw_pct):.2f}%"
            green_text = "color: #2e7d32; font-weight: 600; text-align: center; margin: 0;"
            r1.markdown(f"<p style='{green_text}'>{row['Rank']}</p>", unsafe_allow_html=True)
            r2.markdown(f"<p style='{green_text}'>{row['Ticker']}</p>", unsafe_allow_html=True)
            r3.markdown(f"<p style='{green_text}'>{formatted_shares}</p>", unsafe_allow_html=True)
            r4.markdown(f"<p style='{green_text}'>{formatted_pct}</p>", unsafe_allow_html=True)


# -----------------------------
# Top 5 Decreases
# -----------------------------
top_decreases = data.sort_values("% Change", ascending=True).head(5).copy()
top_decreases.insert(0, "Rank", range(1, len(top_decreases)+1))
top_decreases['Change in Shares'] = (top_decreases['Change in Shares'] / 1e6).round(2).astype(str) + "M"

# -----------------------------
# Top Decreases Table 
# -----------------------------
with right_col:
    with st.container(border=True):
        st.markdown("<h3 style='margin:0; color: #d32f2f;'>Top Decreases</h3>", unsafe_allow_html=True)
        
        h1, h2, h3, h4 = st.columns([1, 1, 1, 1])
        h_style = "color:white; font-weight:bold; text-align:center; margin:0;"
        h1.markdown(f"<p style='{h_style}'>Rank</p>", unsafe_allow_html=True)
        h2.markdown(f"<p style='{h_style}'>Ticker</p>", unsafe_allow_html=True)
        h3.markdown(f"<p style='{h_style}'>Change</p>", unsafe_allow_html=True)
        h4.markdown(f"<p style='{h_style}'>% Change</p>", unsafe_allow_html=True)
        
        st.markdown("<hr style='margin:10px 0; border:0.5px solid #333;'>", unsafe_allow_html=True)

        for _, row in top_decreases.iterrows():
            r1, r2, r3, r4 = st.columns([1, 1, 1, 1])
            raw_shares = str(row["Change in Shares"]).replace('M', '')
            formatted_shares = f"-{float(raw_shares):.2f}M" 
            raw_pct = str(row["% Change"]).replace("%", "").replace("-", "")
            formatted_pct = f"▼ -{float(raw_pct):.2f}%"
            red_text = "color: #d32f2f; font-weight: 600; text-align: center; margin: 0;"
            r1.markdown(f"<p style='{red_text}'>{row['Rank']}</p>", unsafe_allow_html=True)
            r2.markdown(f"<p style='{red_text}'>{row['Ticker']}</p>", unsafe_allow_html=True)
            r3.markdown(f"<p style='{red_text}'>{formatted_shares}</p>", unsafe_allow_html=True)
            r4.markdown(f"<p style='{red_text}'>{formatted_pct}</p>", unsafe_allow_html=True)


# -----------------------------
# Search Filters
# -----------------------------
if 'Stock' not in data.columns:
    data['Stock'] = data['Ticker']

if 'Sector' not in data.columns:
    sectors = ["Technology", "Financials", "Consumer", "Healthcare", "Energy"]
    data['Sector'] = [sectors[i % len(sectors)] for i in range(len(data))]

if 'History' not in data.columns:
    data['History'] = data['Ticker'].apply(lambda x: f"https://finance.yahoo.com/quote/{x}")

for col in ['Shares Held', 'Market Value', '% of Portfolio', 'Change in Shares', '% Change']:
    if col not in data.columns:
        data[col] = 0.0

st.write("")
st.write("")

search_col, filter_col = st.columns([2, 3])
with search_col:
    search = st.text_input("Search holdings", placeholder="🔍 Search ticker or stock...", label_visibility="collapsed")

with filter_col:
    f1, f2, f3, f4 = st.columns([1, 2, 2, 2])
    f1.markdown("<p style='margin-top:10px;'><b>Filter By:</b></p>", unsafe_allow_html=True)
    
    sector_list = ["All Sectors"] + sorted(list(data['Sector'].unique()))
    selected_sector = f2.selectbox("Sector", sector_list, label_visibility="collapsed")
    
    f3.selectbox("% Portfolio", ["All %", "> 5%", "> 10%"], label_visibility="collapsed")
    f4.selectbox("Top N", ["Top 50", "Top 100", "All"], label_visibility="collapsed")

filtered_data = data.copy()
if search:
    filtered_data = filtered_data[
        filtered_data['Ticker'].str.contains(search.upper(), na=False) | 
        filtered_data['Stock'].str.contains(search, case=False, na=False)
    ]
if selected_sector != "All Sectors":
    filtered_data = filtered_data[filtered_data['Sector'] == selected_sector]

# -----------------------------
# Institution Holdings Table
# -----------------------------

with st.container(border=True):
    col_weights = [1.0, 1.8, 1.0, 1.5, 1.2, 1.2, 1.0, 1.2, 1.0]
    cols = st.columns(col_weights)
    h_style = "color: #bdc3c7; font-weight: bold; text-align: center; margin: 0; font-size: 12px; text-transform: uppercase;"
    headers = ["Ticker", "Stock", "History", "Sector", "Shares Held", "Market Value", "% Portfolio", "Change", "% Change"]

    for col, header in zip(cols, headers):
        col.markdown(f"<p style='{h_style}'>{header}</p>", unsafe_allow_html=True)
    st.markdown("<hr style='margin: 5px 0 2px 0; border: 0.5px solid rgba(255,255,255,0.1);'>", unsafe_allow_html=True)

    if filtered_data.empty:
        st.info("No records found.")
    else:
        for _, row in filtered_data.iterrows():
            r = st.columns(col_weights)
            is_pos = row['% Change'] >= 0
            perf_color = "#2e7d32" if is_pos else "#d32f2f"
            arrow = "▲ +" if is_pos else "▼ "
            base_s = "color: white; text-align: center; margin: 0; font-size: 14px; line-height: 1.2;"
            perf_s = f"color: {perf_color}; font-weight: 600; text-align: center; margin: 0; font-size: 14px; line-height: 1.2;"

            r[0].markdown(f"<p style='{base_s} font-weight:bold;'>{row['Ticker']}</p>", unsafe_allow_html=True)
            r[1].markdown(f"<p style='{base_s}'>{row['Stock']}</p>", unsafe_allow_html=True)
            r[2].markdown(f"<div style='text-align:center;'><a href='{row['History']}' target='_blank' style='color:#3498db; text-decoration:none; font-size:13px;'>History ↗</a></div>", unsafe_allow_html=True)
            r[3].markdown(f"<p style='{base_s}'>{row['Sector']}</p>", unsafe_allow_html=True)
            r[4].markdown(f"<p style='{base_s}'>{row['Shares Held']:,.0f}</p>", unsafe_allow_html=True)
            r[5].markdown(f"<p style='{base_s}'>${row['Market Value']:,.0f}</p>", unsafe_allow_html=True)
            r[6].markdown(f"<p style='{base_s}'>{row['% of Portfolio']:.2f}%</p>", unsafe_allow_html=True)
            r[7].markdown(f"<p style='{base_s}'>{row['Change in Shares']:+,.0f}</p>", unsafe_allow_html=True)
            r[8].markdown(f"<p style='{perf_s}'>{arrow}{abs(row['% Change']):.2f}%</p>", unsafe_allow_html=True)
            st.markdown("<hr style='margin: 2px 0; border: 0.1px solid rgba(255,255,255,0.05);'>", unsafe_allow_html=True)