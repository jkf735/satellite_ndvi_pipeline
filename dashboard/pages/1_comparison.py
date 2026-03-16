import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dashboard.db import query

st.set_page_config(page_title="Park Comparison", layout="wide")
st.title("Park Comparison")
st.markdown("Side-by-side vegetation health across all three parks.")

# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    monthly = query("""
        SELECT m.park_code, p.unit_name, m.year, m.month, m.avg_ndvi
        FROM marts.mart_ndvi_monthly m
        JOIN analytics.dim_park p ON m.park_code = p.park_code
        ORDER BY m.park_code, m.year, m.month
    """)
    declining = query("""
        SELECT d.park_code, p.unit_name, d.ndvi_slope, d.trend_label
        FROM marts.mart_declining_parks d
        JOIN analytics.dim_park p ON d.park_code = p.park_code
    """)
    anomalies = query("""
        SELECT park_code, COUNT(*) AS anomaly_count
        FROM marts.mart_ndvi_anomalies
        WHERE ABS(z_score) > 2
        GROUP BY park_code
    """)
    return monthly, declining, anomalies

monthly_df, declining_df, anomaly_df = load_data()

# ── Stat Cards ────────────────────────────────────────────────────────────────

st.markdown("### Overview")
cols = st.columns(3)
for i, row in declining_df.iterrows():
    park_anomalies = anomaly_df[anomaly_df["park_code"] == row["park_code"]]
    anomaly_count = int(park_anomalies["anomaly_count"].values[0]) if not park_anomalies.empty else 0
    mean_ndvi = monthly_df[monthly_df["park_code"] == row["park_code"]]["avg_ndvi"].mean()

    trend_emoji = {"declining": "📉", "improving": "📈", "stable": "➡️"}.get(row["trend_label"], "➡️")
    delta_color = "normal" if row["trend_label"] == "improving" else "inverse" if row["trend_label"] == "declining" else "off"
    delta_arrow = "up" if row["trend_label"] == "improving" else "down" if row["trend_label"] == "declining" else "off"

    with cols[i % 3]:
        st.metric(label=row["unit_name"], value=f"{mean_ndvi:.3f}", delta=f"{row['trend_label']} {trend_emoji}",delta_color=delta_color, delta_arrow=delta_arrow)
        st.caption(f"Anomalous months: {anomaly_count}")

st.markdown("---")

# ── Mean NDVI by Year ─────────────────────────────────────────────────────────

st.markdown("### Mean NDVI by Year")
yearly = monthly_df.groupby(["park_code", "unit_name", "year"])["avg_ndvi"].mean().reset_index()
fig = px.bar(
    yearly,
    x="year",
    y="avg_ndvi",
    color="unit_name",
    barmode="group",
    labels={"avg_ndvi": "Mean NDVI", "year": "Year", "unit_name": "Park"},
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    legend_title_text="Park"
)
st.plotly_chart(fig, width='stretch')

# ── NDVI Distribution ─────────────────────────────────────────────────────────

st.markdown("### NDVI Distribution")
fig2 = px.box(
    monthly_df,
    x="unit_name",
    y="avg_ndvi",
    color="unit_name",
    labels={"avg_ndvi": "Monthly NDVI", "unit_name": "Park"},
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig2.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    showlegend=False
)
st.plotly_chart(fig2, width='stretch')