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
    st.markdown("Sierra Nevada alpine ecosystem — snowmelt-driven spring green-up")
with col2:
    st.markdown("### 🏜️ Zion")
    st.markdown("Desert canyon — low NDVI baseline, arid seasonality")
with col3:
    st.markdown("### 🌊 Acadia")
    st.markdown("Coastal New England — deciduous, classic summer peak")