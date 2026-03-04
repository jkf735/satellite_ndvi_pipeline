"""
clip_to_park.py
Clips interim NDVI.tif file to just park outline

Inputs: 
   - Park_name, year, month
   - coresponding NDVI.tif file to be present in data/interim/{park_name} (name example: 2025_11_2_NDVI.tif)
Outputs: 
   - clipped NDVI.tif file created in data/iprocessed (name example: yosemite_2025_11_2_NDVI.tif)

Usage:
    python3 clip_to_park.py --park yosemite --year 2025 --month 11
    make clip PARK=Yosemite YEAR=2025 MONTH=11
"""
import os
import logging
import argparse
import rasterio
import numpy as np
import geopandas as gpd
from rasterio.mask import mask
from rasterio.features import geometry_mask
from sqlalchemy import create_engine, text

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
    Clip NDVI raster to park boundary and write GeoTIFF.

    Parameters
    ----------
    park_name : str
        name of park
    ndvi_path : str
        Path NDVI.tif
    output_path : str
        Path where clipped GeoTIFF will be written

    Returns
    ----------
    bool : If clipping was completed or not
    """
    if os.path.exists(output_path):
        logger.warning(f'Clipped NDVI file already exist at {output_path}. If you want to clip again, delete the current raster and run again.')
        return False
    
    # connect to PostGIS
    logger.info("Connectiong to PostGIS database...")
    engine = create_engine(DB_URI)

    # pull park boundary
    logger.info(f"Querying 'parks_validated' table for park_name like '{park_name}%...")
    query = f"""
        SELECT geom
        FROM parks_validated
        WHERE park_name ILIKE '{park_name}%'
    """
    park_gdf = gpd.read_postgis(text(query), engine, geom_col="geom")
    if park_gdf.empty:
        logging.error(f"No {park_name} geometry found in database.")
        raise ValueError(f"No {park_name} geometry found in database.")
    
    logger.info("Reading Inputs...")
    with rasterio.open(ndvi_path) as src:
        # reproject park to raster CRS
        raster_crs = src.crs
        if park_gdf.crs != raster_crs:
            park_gdf = park_gdf.to_crs(raster_crs)

        # extract geometry as GeoJSON-like mapping
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

        # run QA:
        logger.info("Running Clipped NDVI QA...")
        mask_array = geometry_mask(
            park_gdf.geometry,
            transform=clipped_transform,
            invert=True,
            all_touched=True,
            out_shape=clipped_array.shape
        )
        clip_qa(clipped_array, mask_array)

    # save
    logger.info("Writting To Output...")
    with rasterio.open(output_path, "w", **profile) as dest:
        dest.write(clipped_array,1)

    logger.info(f"Clipped NDVI raster Successfully written to: {output_path}")
    return True

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
    ouput_file = park.lower() + '_' + ndvi_file
    output_path = PROCESSED_DATA_DIR / ouput_file
    complete = clip_ndvi_to_park(park, ndvi_path, output_path)
    if complete:
        logging.info(f'COMPLETED CLIPPING {park}, {year}, {month}')
    else:
        logging.info(f'CLIPPING SPKIPPED: file alread exists for {park}, {year}, {month} at {output_path}')

if __name__ == "__main__":
    main()