import streamlit as st
import plotly.express as px
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dashboard.db import query

st.set_page_config(page_title="Anomalies", layout="wide")
st.title("NDVI Anomalies")
st.markdown("Months where NDVI deviated significantly from the park's historical average (|z-score| > 2).")

# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    return query("""
        SELECT a.park_code, p.unit_name, a.date_key, a.mean_ndvi, a.z_score,
               CASE WHEN a.z_score > 0 THEN 'Above Average' ELSE 'Below Average' END AS direction
        FROM marts.mart_ndvi_anomalies a
        JOIN analytics.dim_park p ON a.park_code = p.park_code
        WHERE ABS(a.z_score) > 2
        ORDER BY ABS(a.z_score) DESC
    """)

df = load_data()

if df.empty:
    st.info("No anomalies detected across the dataset.")
else:
    # ── Filters ───────────────────────────────────────────────────────────────

    parks = ["All"] + sorted(df["unit_name"].unique().tolist())
    selected_park = st.sidebar.selectbox("Filter by Park", parks)
    if selected_park != "All":
        df = df[df["unit_name"] == selected_park]

    # ── Summary ───────────────────────────────────────────────────────────────

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Anomalies", len(df))
    col2.metric("Above Average", len(df[df["direction"] == "Above Average"]))
    col3.metric("Below Average", len(df[df["direction"] == "Below Average"]))

    st.markdown("---")

    # ── Chart ─────────────────────────────────────────────────────────────────

    fig = px.scatter(
        df,
        x="date_key",
        y="z_score",
        color="unit_name",
        symbol="direction",
        size=df["z_score"].abs(),
        hover_data=["mean_ndvi", "z_score"],
        labels={"date_key": "Date", "z_score": "Z-Score", "unit_name": "Park"},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.add_hline(y=2, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_hline(y=-2, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("### Anomaly Detail")
    st.dataframe(
        df[["unit_name", "date_key", "mean_ndvi", "z_score", "direction"]]
        .rename(columns={"unit_name": "Park", "date_key": "Date",
                         "mean_ndvi": "Mean NDVI", "z_score": "Z-Score",
                         "direction": "Direction"})
        .reset_index(drop=True),
        use_container_width=True
    )