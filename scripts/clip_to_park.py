import os
import rasterio
import geopandas as gpd
from rasterio.mask import mask
from sqlalchemy import create_engine, text

from resources.config import DB_URI, PROCESSED_DATA_DIR
#TODO previous script: combine into 1 stitch
#TODO this file: check that stitch exists for park,year,month, take stitch and compute ndvi, then clip to park.
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

    with rasterio.open(PROCESSED_DATA_DIR / "2025-11-14_ndvi.tif") as src:
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

    with rasterio.open(PROCESSED_DATA_DIR / "2025-11-14_ndvi_yosemite.tif", "w", **out_meta) as dest:
        dest.write(out_image)

    print(f"Clipped NDVI written to {PROCESSED_DATA_DIR / "2025-11-14_ndvi_yosemite.tif"}")


if __name__ == "__main__":
    clip_ndvi_to_park('Yosemite')