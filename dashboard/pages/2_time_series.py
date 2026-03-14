import streamlit as st
import plotly.graph_objects as go
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dashboard.db import query

st.set_page_config(page_title="Time Series", layout="wide")
st.title("NDVI Time Series")
st.markdown("Monthly NDVI trends with 6-month rolling average and anomaly overlay.")

# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    trend = query("""
        SELECT t.park_code, p.unit_name, t.date_key, t.mean_ndvi, t.rolling_6mo_avg
        FROM marts.mart_ndvi_trend t
        JOIN analytics.dim_park p ON t.park_code = p.park_code
        ORDER BY t.park_code, t.date_key
    """)
    anomalies = query("""
        SELECT a.park_code, a.date_key, a.mean_ndvi, a.z_score
        FROM marts.mart_ndvi_anomalies a
        WHERE ABS(a.z_score) > 2
    """)
    return trend, anomalies

trend_df, anomaly_df = load_data()

# ── Park Selector ─────────────────────────────────────────────────────────────

parks = trend_df[["park_code", "unit_name"]].drop_duplicates()
park_options = dict(zip(parks["unit_name"], parks["park_code"]))
selected_name = st.sidebar.selectbox("Select Park", list(park_options.keys()))
selected_code = park_options[selected_name]

park_trend = trend_df[trend_df["park_code"] == selected_code]
park_anomalies = anomaly_df[anomaly_df["park_code"] == selected_code]

# ── Chart ─────────────────────────────────────────────────────────────────────

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=park_trend["date_key"],
    y=park_trend["mean_ndvi"],
    mode="lines",
    name="Monthly NDVI",
    line=dict(color="#74c476", width=1.5, dash="dot"),
    opacity=0.7
))

fig.add_trace(go.Scatter(
    x=park_trend["date_key"],
    y=park_trend["rolling_6mo_avg"],
    mode="lines",
    name="6-Month Rolling Avg",
    line=dict(color="#238b45", width=2.5)
))

if not park_anomalies.empty:
    fig.add_trace(go.Scatter(
        x=park_anomalies["date_key"],
        y=park_anomalies["mean_ndvi"],
        mode="markers",
        name="Anomaly",
        marker=dict(color="#d62728", size=10, symbol="x")
    ))

fig.update_layout(
    title=f"{selected_name} — Monthly NDVI (2022–2026)",
    xaxis_title="Date",
    yaxis_title="NDVI",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02)
)
st.plotly_chart(fig, use_container_width=True)