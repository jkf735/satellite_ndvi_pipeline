import os
import streamlit as st
from pathlib import Path
import subprocess
import sys
import requests
import threading

TITILER_URL = os.getenv("TITILER_URL", "http://localhost:8001")

# Load warehouse if it doesn't exit
warehouse_path = Path(__file__).parent.parent / "warehouse" / "warehouse.db"

if not warehouse_path.exists():
    st.info("Building warehouse for first time... this may take a minute.")
    result = subprocess.run(
        [sys.executable, "quickstart.py"],
        capture_output=False
    )
    if result.returncode != 0:
        st.error("Failed to build warehouse.")
        st.stop()
    st.rerun()

st.set_page_config(
    page_title="National Parks NDVI Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ping titiler in background so map page is ready sooner
def ping_titiler():
    """Ping Titiler in background to wake up Render instance."""
    try:
        requests.get(f"{TITILER_URL}/healthz", timeout=30)
    except Exception:
        pass  # silent fail — just a warm-up ping

# fire and forget on page load
thread = threading.Thread(target=ping_titiler, daemon=True)
thread.start()


st.title("🌿 National Parks NDVI Dashboard")
st.markdown("""
Vegetation health monitoring across **Yosemite**, **Zion**, and **Acadia** National Parks
using Sentinel-2 satellite imagery (2022–2026).

Use the sidebar to navigate between views.
""")

st.markdown("---")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("### 🏔️ Yosemite")
    st.markdown("Diverse Sierra Nevada ecosystem — granite cliffs, giant sequoia groves, alpine meadows, and coniferous forests")
with col2:
    st.markdown("### 🏜️ Zion")
    st.markdown("Desert canyon — desert, riparian, woodland, and coniferous forest")
with col3:
    st.markdown("### 🦞 Acadia")
    st.markdown("Coastal New England — northern coniferous and eastern deciduous forests")

st.markdown("---")  

st.markdown("### Current Limitations")

st.markdown("""
    - In 2022 2022 ESA changed the Sentinel-2 L2A processing baseline from N0400 to N0500. 
    This introduced a change in how surface reflectance is computed, specifically a offset correction was added to the bottom of atmosphere reflectance values.
    **Currently this project is not taking that correction into account so data pre 2022 is being suppressed.**
    - Each monthly datapoint is determined by a single value from that month. Averaging was not used as it was unlikely to find more than 1 or 2 tile-sets with low cloud coverage.
      - Tile selection relies on CLOUDY_PIXEL_PERCENTAGE from sentinel-2 metadata.xml. This metric does not capture all atmospheric interference 
""")

st.markdown("## Pipeline Stages")
st.markdown("Visual progression from raw Sentinel-2 bands to final clipped NDVI raster.")

col1, col2, col3, col4 = st.columns(4)
static = Path(__file__).parent / "static"

with col1:
    st.image(str(static / "b04.png"), caption="B04 — Red Band", width='stretch')
with col2:
    st.image(str(static / "b08.png"), caption="B08 — NIR Band", width='stretch')
with col3:
    st.image(str(static / "ndvi.png"), caption="NDVI (Full Tile)", width='stretch')
with col4:
    st.image(str(static / "ndvi_clipped.png"), caption="NDVI (Clipped to Park)", width='stretch')