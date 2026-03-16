import streamlit as st

st.set_page_config(
    page_title="National Parks NDVI Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

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