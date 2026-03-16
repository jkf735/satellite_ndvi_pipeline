import os
import streamlit as st
from streamlit_folium import st_folium
import folium
import requests
import json
import boto3
import pandas as pd
import branca.colormap as cm
from botocore import UNSIGNED
from botocore.config import Config
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dashboard.db import query
from scripts.resources.config import S3_BUCKET_NAME

st.set_page_config(page_title="NDVI Map", layout="wide")
st.title("NDVI Map")
st.markdown("Spatial NDVI distribution streamed directly from S3 COGs via Titiler.")

TITILER_URL = os.getenv("TITILER_URL", "http://localhost:8001")
COLORMAP = "rdylgn"
RESCALE = "-0.2,0.8"

# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data
def load_available_dates():
    return query("""
        SELECT p.unit_name, p.park_code, d.year, d.month, f.source_raster
        FROM analytics.fact_ndvi f
        JOIN analytics.dim_park p ON f.park_code = p.park_code
        JOIN analytics.dim_date d ON f.date_key = d.date_key
        ORDER BY p.unit_name, d.year, d.month
    """)

@st.cache_data
def load_park_boundary(park_name: str):
    """Download NPS boundary GeoJSON from S3 and filter to selected park."""
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="nps_boundary.geojson")
    geojson = json.loads(obj["Body"].read())

    # filter to selected park
    features = [
        f for f in geojson["features"]
        if park_name.lower() in f["properties"].get("UNIT_NAME", "").lower()
    ]
    return {"type": "FeatureCollection", "features": features}

@st.cache_data
def load_stats(park_code: str, year: int, month: int):
    return query(f"""
        SELECT f.mean_ndvi, f.std_ndvi, a.z_score
        FROM analytics.fact_ndvi f
        JOIN analytics.dim_date d ON f.date_key = d.date_key
        LEFT JOIN marts.mart_ndvi_anomalies a 
            ON f.park_code = a.park_code AND f.date_key = a.date_key
        WHERE f.park_code = '{park_code}'
        AND d.year = {year}
        AND d.month = {month}
    """)

df = load_available_dates()

# ── Selectors ─────────────────────────────────────────────────────────────────

parks = sorted(df["unit_name"].unique().tolist())
selected_park = st.sidebar.selectbox("Select Park", parks)
park_df = df[df["unit_name"] == selected_park]
park_code = park_df["park_code"].iloc[0]

years = sorted(park_df["year"].unique().tolist(), reverse=True)
selected_year = st.sidebar.selectbox("Select Year", years)
year_df = park_df[park_df["year"] == selected_year]

months = sorted(year_df["month"].unique().tolist())
MONTH_LABELS = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
selected_month = st.sidebar.selectbox(
    "Select Month",
    months,
    format_func=lambda x: MONTH_LABELS[x]
)

opacity = st.sidebar.slider("Layer Opacity", 0.0, 1.0, 0.8, 0.1)

# ── COG URL ───────────────────────────────────────────────────────────────────

selected_row = year_df[year_df["month"] == selected_month].iloc[0]
source_raster = selected_row["source_raster"]
park_name_lower = selected_park.lower().split()[0]
s3_url = f"s3://{S3_BUCKET_NAME}/processed/{park_name_lower}/{source_raster}"

# ── Tile URL + Bounds ─────────────────────────────────────────────────────────

@st.cache_data
def get_cog_bounds(s3_url):
    resp = requests.get(
        f"{TITILER_URL}/cog/WebMercatorQuad/tilejson.json",
        params={"url": s3_url, "colormap_name": COLORMAP, "rescale": RESCALE}
    )
    return resp.json()

tilejson = get_cog_bounds(s3_url)
bounds = tilejson["bounds"]  # [west, south, east, north]

tile_url = (
    f"{TITILER_URL}/cog/tiles/WebMercatorQuad/"
    + "{z}/{x}/{y}"
    + f"?url={s3_url}&colormap_name={COLORMAP}&rescale={RESCALE}&tilesize=512"
)

# ── Layout ────────────────────────────────────────────────────────────────────

map_col, stats_col = st.columns([3, 1])

with map_col:
    st.markdown(f"**{selected_park} — {MONTH_LABELS[selected_month]} {selected_year}**")

    m = folium.Map(tiles="CartoDB dark_matter")
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

    # NDVI tile layer
    folium.TileLayer(
        tiles=tile_url,
        attr="Sentinel-2 NDVI",
        name="NDVI",
        overlay=True,
        control=True,
        opacity=opacity
    ).add_to(m)

    # park boundary
    try:
        boundary = load_park_boundary(selected_park)
        if boundary["features"]:
            folium.GeoJson(
                boundary,
                name="Park Boundary",
                style_function=lambda x: {
                    "fillColor": "transparent",
                    "color": "white",
                    "weight": 2,
                    "dashArray": "5 5"
                }
            ).add_to(m)
    except Exception as e:
        st.caption(f"Could not load park boundary: {e}")

    # colorscale legend
    colorscale = cm.LinearColormap(
        colors=["red", "yellow", "green"],
        vmin=-0.2, vmax=0.8,
        caption="NDVI"
    )
    colorscale.add_to(m)

    folium.LayerControl().add_to(m)
    st_folium(
        m,
        width=None,
        height=600,
        returned_objects=[],
        key=f"{selected_park}_{selected_year}_{selected_month}"
    )

with stats_col:
    st.markdown("### Stats")
    stats_df = load_stats(park_code, selected_year, selected_month)

    if not stats_df.empty:
        row = stats_df.iloc[0]
        mean_ndvi = row["mean_ndvi"]
        std_ndvi = row["std_ndvi"]
        z_score = row["z_score"]

        st.metric("Mean NDVI", f"{mean_ndvi:.3f}")
        st.metric("Std Dev", f"{std_ndvi:.3f}")

        if pd.isna(z_score):
            st.metric("Z-Score", "N/A")
            st.caption("Insufficient data for anomaly detection")
        else:
            is_anomaly = abs(z_score) > 1.5
            st.metric(
                "Z-Score",
                f"{z_score:.2f}",
                delta="anomaly" if is_anomaly else "normal",
                delta_color="inverse" if is_anomaly else "off"
            )

        st.markdown("---")
        st.caption(f"Source: `{source_raster}`")
        st.caption(f"COG: `{s3_url}`")
    else:
        st.warning("No stats available for this selection.")