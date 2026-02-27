import os
import rasterio
import geopandas as gpd
from rasterio.mask import mask
from sqlalchemy import create_engine, text

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

NDVI_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "2025-11-14_ndvi.tif")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "2025-11-14_ndvi_yosemite.tif")

# Update with your PostGIS connection
DB_URI = "postgresql://geo_user:geo_pass@localhost:5432/geo"


def clip_ndvi_to_park(park_name):
    # Connect to PostGIS
    engine = create_engine(DB_URI)

    # Pull Yosemite boundary
    query = f"""
        SELECT geom
        FROM parks_validated
        WHERE park_name ILIKE '{park_name}%'
    """

    gdf = gpd.read_postgis(text(query), engine, geom_col="geom")
    print(gdf)

    if gdf.empty:
        raise ValueError("No Yosemite geometry found in database.")

    with rasterio.open(NDVI_PATH) as src:
        raster_crs = src.crs

        # Reproject park to raster CRS
        gdf = gdf.to_crs(raster_crs)

        # Extract geometry as GeoJSON-like mapping
        geometries = [geom.__geo_interface__ for geom in gdf.geometry]
        print("Raster bounds:", src.bounds)
        print("Raster CRS:", src.crs)
        print("Park CRS:", gdf.crs)
        print("Park bounds:", gdf.total_bounds)
        # Clip
        out_image, out_transform = mask(src, geometries, crop=True)

        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

    with rasterio.open(OUTPUT_PATH, "w", **out_meta) as dest:
        dest.write(out_image)

    print(f"Clipped NDVI written to {OUTPUT_PATH}")


if __name__ == "__main__":
    clip_ndvi_to_park('Yosemite')