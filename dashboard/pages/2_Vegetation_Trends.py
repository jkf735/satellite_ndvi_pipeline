import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dashboard.db import query

st.set_page_config(page_title="Vegetation Trends", layout="wide")
st.title("Vegetation Trends")
st.markdown("Monthly NDVI time series and seasonal patterns across all three parks.")

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
        WHERE ABS(a.z_score) > 1.5
    """)
    seasonality = query("""
        SELECT s.park_code, p.unit_name, s.month, s.seasonal_avg_ndvi
        FROM marts.mart_ndvi_seasonality s
        JOIN analytics.dim_park p ON s.park_code = p.park_code
        ORDER BY s.park_code, s.month
    """)
    return trend, anomalies, seasonality

trend_df, anomaly_df, seasonality_df = load_data()

# ── Park Selector ─────────────────────────────────────────────────────────────

parks = trend_df[["park_code", "unit_name"]].drop_duplicates()
park_options = dict(zip(parks["unit_name"], parks["park_code"]))
selected_name = st.sidebar.selectbox("Select Park", list(park_options.keys()))
selected_code = park_options[selected_name]

park_trend = trend_df[trend_df["park_code"] == selected_code]
park_anomalies = anomaly_df[anomaly_df["park_code"] == selected_code]

# ── Time Series ───────────────────────────────────────────────────────────────

st.markdown("### NDVI Time Series")
st.markdown("Monthly NDVI trends with 6-month rolling average and anomaly overlay.")

fig_ts = go.Figure()

fig_ts.add_trace(go.Scatter(
    x=park_trend["date_key"],
    y=park_trend["mean_ndvi"],
    mode="lines",
    name="Monthly NDVI",
    line=dict(color="#74c476", width=1.5, dash="dot"),
    opacity=0.7
))

fig_ts.add_trace(go.Scatter(
    x=park_trend["date_key"],
    y=park_trend["rolling_6mo_avg"],
    mode="lines",
    name="6-Month Rolling Avg",
    line=dict(color="#238b45", width=2.5)
))

if not park_anomalies.empty:
    fig_ts.add_trace(go.Scatter(
        x=park_anomalies["date_key"],
        y=park_anomalies["mean_ndvi"],
        mode="markers",
        name="Anomaly",
        marker=dict(color="#d62728", size=10, symbol="x")
    ))

fig_ts.add_vline(
    x=pd.Timestamp("2022-01-26").timestamp() * 1000,
    line_dash="dash",
    line_color="rgba(255,255,255,0.4)",
    line_width=1.5,
    annotation_text="ESA Processing Baseline Change (N0400→N0500)",
    annotation_position="top right",
    annotation_font=dict(size=10, color="rgba(255,255,255,0.6)")
)

fig_ts.update_layout(
    title=f"{selected_name} — Monthly NDVI",
    xaxis_title="Date",
    yaxis_title="NDVI",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02)
)
st.plotly_chart(fig_ts, use_container_width=True)

st.markdown("---")

# ── Seasonality ───────────────────────────────────────────────────────────────

st.markdown("### Seasonal Patterns")
st.markdown("Average NDVI by month across all parks — highlights ecological contrast between ecosystems.")

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
COLORS = {
    "Yosemite National Park": "#2ca02c",
    "Zion National Park": "#d62728",
    "Acadia National Park": "#1f77b4"
}

fig_season = go.Figure()
for park_name, group in seasonality_df.groupby("unit_name"):
    group = group.sort_values("month")
    fig_season.add_trace(go.Scatter(
        x=group["month"],
        y=group["seasonal_avg_ndvi"],
        mode="lines+markers",
        name=park_name,
        line=dict(width=2.5, color=COLORS.get(park_name)),
        marker=dict(size=8)
    ))

fig_season.update_layout(
    xaxis=dict(
        tickmode="array",
        tickvals=list(range(1, 13)),
        ticktext=MONTH_LABELS,
        title="Month"
    ),
    yaxis_title="Average NDVI",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    legend_title_text="Park"
)
st.plotly_chart(fig_season, use_container_width=True)