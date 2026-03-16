import streamlit as st
import plotly.graph_objects as go
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dashboard.db import query

st.set_page_config(page_title="Seasonality", layout="wide")
st.title("Seasonal NDVI Patterns")
st.markdown("Average NDVI by month across all parks.")

# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    return query("""
        SELECT s.park_code, p.unit_name, s.month, s.seasonal_avg_ndvi
        FROM marts.mart_ndvi_seasonality s
        JOIN analytics.dim_park p ON s.park_code = p.park_code
        ORDER BY s.park_code, s.month
    """)

df = load_data()

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
COLORS = {"Yosemite National Park": "#2ca02c", "Zion National Park": "#d62728", "Acadia National Park": "#1f77b4"}

# ── Chart ─────────────────────────────────────────────────────────────────────

fig = go.Figure()
for park_name, group in df.groupby("unit_name"):
    group = group.sort_values("month")
    fig.add_trace(go.Scatter(
        x=group["month"],
        y=group["seasonal_avg_ndvi"],
        mode="lines+markers",
        name=park_name,
        line=dict(width=2.5, color=COLORS.get(park_name)),
        marker=dict(size=8)
    ))

fig.update_layout(
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
st.plotly_chart(fig, width='stretch')