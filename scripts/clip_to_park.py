"""
clip_to_park.py
Clips interim NDVI.tif file to just park outline

Inputs: 
   - Park_name, year, month
   - corresponding NDVI.tif file to be present in data/interim/{park_name} (name example: 2025_11_2_NDVI.tif)
Outputs: 
   - clipped NDVI.tif file created in data/processed (name example: yosemite_2025_11_2_NDVI.tif)

Usage:
    python3 clip_to_park.py --park yosemite --year 2025 --month 11
    make clip PARK=Yosemite YEAR=2025 MONTH=11
"""
import os
import logging
import argparse
import rasterio
from pathlib import Path
from shapely import wkb
import numpy as np
import geopandas as gpd
from rasterio.mask import mask
from rasterio.features import geometry_mask
from rasterio.enums import Resampling
import rasterio.shutil as rio_shutil
from rio_cogeo.cogeo import cog_validate
from db import get_db_connection

from resources.config import DB_URI, INTERIM_DATA_DIR, PROCESSED_DATA_DIR

logger = logging.getLogger("clip_to_park")

def find_ndvi(folder, year, month) -> str:
    """
    Look for the NDVI .tif file in a given folder for a give year, month and return its path

    Parameters
    ----------
    folder:
        path to interim/{park} folder
    year: int or str
        year being looked at (e.g. 2025)
    month: int or str
        month being looked at (e.g. 11)
        
    Returns
    ----------
    ndvi_file_name: str
    """
    year = str(year)
    month = str(month)
    if not os.path.isdir(folder):
        logger.warning(f"Input folder not found: {folder}")
        raise FileNotFoundError(f"Input folder not found: {folder}")
    files = sorted(os.listdir(folder))
    ndvi_file = ""
    for file in files:
        split = file.split('_')
        file_year = split[0]
        file_month = split[1]
        file_band = split[3]
        if file_year ==  str(year) and file_month == str(month):
            if file_band == "NDVI.tif": ndvi_file = file

    if ndvi_file == "":
        logger.error(f"ndvi file not found in: {folder} for year:{year}, month:{month}")
        raise FileNotFoundError(f"B04 (red) file not found in: {folder} for year:{year}, month:{month}")

    logger.info(f"Found input files: {folder / ndvi_file}")
    return ndvi_file

def clip_ndvi_to_park(park_name: str, ndvi_path, output_path) -> bool:
    """
    Clip NDVI raster to park boundary and write as Cloud Optimized GeoTIFF (COG).

    Parameters
    ----------
    park_name : str
        name of park
    ndvi_path : str
        Path NDVI.tif
    output_path : str
        Path where clipped COG will be written

    Returns
    ----------
    bool : If clipping was completed or not
    """
    if os.path.exists(output_path):
        logger.warning(f'Clipped NDVI file already exist at {output_path}. If you want to clip again, delete the current raster and run again.')
        return False

    # pull park boundary via psycopg2
    logger.info("Connecting to PostGIS database...")
    logger.info(f"Querying 'parks_validated' table for park_name like '{park_name}%'...")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT ST_AsEWKB(geom) as geom
                FROM parks_validated
                WHERE park_name ILIKE '{park_name}%'
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        logging.error(f"No {park_name} geometry found in database.")
        raise ValueError(f"No {park_name} geometry found in database.")

    geometries_shapely = [wkb.loads(bytes(row[0])) for row in rows]
    park_gdf = gpd.GeoDataFrame(geometry=geometries_shapely, crs="EPSG:4326")

    logger.info("Reading inputs...")
    with rasterio.open(ndvi_path) as src:
        # re-project park to raster CRS
        raster_crs = src.crs
        if park_gdf.crs != raster_crs:
            park_gdf = park_gdf.to_crs(raster_crs)

        geometries = park_gdf.geometry.values

        # clip
        logger.info("Clipping to park boundary...")
        clipped_array, clipped_transform = mask(
            src,
            geometries,
            crop=True,
            all_touched=True
        )
        clipped_array = clipped_array[0]
        profile = src.profile.copy()
        profile.update({
            "height": clipped_array.shape[0],
            "width": clipped_array.shape[1],
            "transform": clipped_transform
        })

        # run QA
        logger.info("Running clipped NDVI QA...")
        mask_array = geometry_mask(
            park_gdf.geometry,
            transform=clipped_transform,
            invert=True,
            all_touched=True,
            out_shape=clipped_array.shape
        )
        clip_qa(clipped_array, mask_array)

    # write COG
    logger.info("Writing COG to output...")
    success = write_cog(clipped_array, profile, output_path)

    if success:
        logger.info(f"Clipped NDVI COG successfully written to: {output_path}")
        return True
    else:
        logger.warning(f"Could NOT write clipped NDVI COG to: {output_path}")
        return False


def write_cog(array: np.ndarray, profile: dict, output_path: Path) -> bool:
    """
    Write a 2D numpy array to disk as a Cloud Optimized GeoTIFF.

    Parameters
    ----------
    array : np.ndarray
        2D array to write
    profile : dict
        Rasterio profile from source raster (will be updated for COG)
    output_path : Path
        Path where COG will be written

    Returns
    ----------
    bool : If COG conversion was completed or not
    """
    retval = True
    cog_profile = profile.copy()
    """ Notes on COG profile:
    - DEFLATE compression (better ratio than LZW for float32 NDVI data)
    - 512x512 internal tiles (standard COG tile size)
    - Overview levels for multi-scale rendering (used later for Titiler/map tile server)
    - Predictor=2 horizontal differencing (improves DEFLATE ratio on continuous data)"""
    cog_profile.update({
        "driver": "GTiff",
        "dtype": "float32",
        "compress": "DEFLATE",
        "predictor": 2,       
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "interleave": "band",
        "count": 1
    })

    # COGs require a temp file first — overviews must be built before the final file is written
    tmp_path = str(output_path).replace(".tif", "_tmp.tif")
    try:
        with rasterio.open(tmp_path, "w", **cog_profile) as tmp:
            tmp.write(array, 1)

            # build overviews — levels 2,4,8,16 cover zoom levels needed by Titiler
            overview_levels = [2, 4, 8, 16]
            tmp.build_overviews(overview_levels, Resampling.average)
            tmp.update_tags(ns="rio_overview", resampling="average")

        # copy to final COG with overviews embedded at file start
        cog_creation_options = {
            "compress": "DEFLATE",
            "predictor": 2,
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "interleave": "band",
        }
        rio_shutil.copy(tmp_path, str(output_path), copy_src_overviews=True, driver="GTiff", **cog_creation_options)
        logger.info(f"COG overviews built at levels: {overview_levels}")
        retval = True
    except:
        logger.error(f"Unexpected error while converting to COG!")
        retval = False
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    if retval == False:
        return retval
    
    # QA the COG
    is_valid, errors, warnings = cog_validate(output_path)
    if is_valid:
        logger.info(f"COG verified")
        return True
    else:
        logger.error(f"COG ERRORS: {errors}")
        logger.warning(f"COG WARNINGS: {warnings}")
        return False

def clip_qa(clipped_array, mask_array) -> dict:
    """
    Perform QA on clipped NDVI 

    Parameters
    ----------
    profile :
        data profile
    clipped_array :
        clipped ndvi array
    Returns
    ----------
    dict : {"min", "max", "mean", "nodata_pct_total", nodata_pct_inside, "valid_pixels"}
    """
    total_pixels = clipped_array.size
    nan_count = np.isnan(clipped_array).sum()
    valid_pixels = total_pixels - nan_count

    if valid_pixels == 0:
        logger.error("No valid NDVI pixels inside park boundary.")
        raise ValueError("No valid NDVI pixels inside park boundary.")

    # nan-aware stats
    ndvi_mean = float(np.nanmean(clipped_array))
    ndvi_min = float(np.nanmin(clipped_array))
    ndvi_max = float(np.nanmax(clipped_array))
    nodata_pct_total = 100 * (nan_count / total_pixels)

    inside_pixels = clipped_array[mask_array]
    inside_nan = np.isnan(inside_pixels).sum()
    nodata_pct_inside = 100 * (inside_nan / inside_pixels.size)
    
    logger.info(
        f"\n--- CLIPPED NDVI QA Summary ---"
        f"\nMin NDVI:  {ndvi_min:.4f}"
        f"\nMax NDVI:  {ndvi_max:.4f}"
        f"\nMean NDVI: {ndvi_mean:.4f}"
        f"\nNodata % (total bounding box):  {nodata_pct_total:.2f}%"
        f"\nNodata % (inside park): {nodata_pct_inside:.2f}%"
        f"\nValid pixels: {valid_pixels}"
        f"\n------------------------"
    )
    
    # sanity checks
    if ndvi_min < -1.1 or ndvi_max > 1.1:
        logger.error("NDVI values outside expected range (-1 to 1)")
        raise ValueError("NDVI values outside expected range (-1 to 1)")

    if nodata_pct_inside > 90:
        logger.error("Too many nodata pixels — clipped raster may be invalid")
        raise ValueError("Too many nodata pixels — clipped raster may be invalid")
    
    return {
        "min_ndvi": ndvi_min,
        "max_ndvi": ndvi_max,
        "mean_ndvi": ndvi_mean,
        "nodata_pct_total": nodata_pct_total,
        "nodata_pct_inside": nodata_pct_inside,
        "valid_pixels": int(valid_pixels)
    }

def main(park=None, year=None, month=None):
    """
    Main function call for clip_to_park.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/tile_processing.log"),
        logging.StreamHandler()
    ]
    )
    if park is None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--park", type=str, required=True)
        parser.add_argument("--year", type=int, required=True)
        parser.add_argument("--month", type=int, required=True)
        args = parser.parse_args()
        park, year, month = args.park, args.year, args.month
    input_folder = INTERIM_DATA_DIR / park.lower()
    
    logging.info(f'CLIPING NDVI FILE TO PARK BOUNDARY: {park}, {year}, {month}')
    ndvi_file = find_ndvi(input_folder, year, month)
    ndvi_path = input_folder / ndvi_file
    output_file = park.lower() + '_' + ndvi_file
    output_path = PROCESSED_DATA_DIR / output_file
    complete = clip_ndvi_to_park(park, ndvi_path, output_path)
    if complete:
        logging.info(f'COMPLETED CLIPPING {park}, {year}, {month}')
    else:
        logging.info('CLIPPING SKIPPED/FAILED: (see above info)')

if __name__ == "__main__":
    main()