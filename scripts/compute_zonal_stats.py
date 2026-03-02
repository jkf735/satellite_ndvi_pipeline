import os
import glob
import argparse
import logging
import numpy as np
import rasterio
import psycopg2
from dotenv import load_dotenv
from datetime import date

from db import get_db_connection
from resources.config import DB_URI, PROCESSED_DATA_DIR

logger = logging.getLogger("compute_zonal_stats")

# COMPUTE LAYER
def compute_zonal_stats(tif_path: str) -> dict:
    """
    Computes stats for given clipped raster

    Parameters
    ----------
    tif_path : str
        path to clipped raster

    Returns
    ----------
    bool: If raster data already in table
    """
    filename = os.path.basename(tif_path)
    name = filename.replace("_NDVI.tif", "")

    # Expect: park_year_month.tif
    try:
        park_name, year, month, day, = name.split("_")
    except ValueError:
        logging.error(f"Filename format must be park_year_month_day_NDVI.tif → got {filename}")
        raise ValueError(f"Filename format must be park_year_month_day_NDVI.tif → got {filename}")

    obs_date = date(int(year), int(month), int(day))

    with rasterio.open(tif_path) as src:
        ndvi = src.read(1)

    valid_values = ndvi[~np.isnan(ndvi)]

    if valid_values.size == 0:
        logging.error(f"No valid pixels in {filename}")
        raise ValueError(f"No valid pixels in {filename}")

    return {
        "park_name": park_name,
        "date": obs_date,
        "year": year,
        "month": month,
        "mean_ndvi": float(np.mean(valid_values)),
        "std_ndvi": float(np.std(valid_values)),
        "min_ndvi": float(np.min(valid_values)),
        "max_ndvi": float(np.max(valid_values)),
        "valid_pixels": int(valid_values.size),
        "source_raster": filename
    }


# DATABASE LAYER
def raster_already_loaded(conn: psycopg2.extensions.connection, source_raster: str) -> bool:
    """
    Checks if rater already used in DB

    Parameters
    ----------
    conn : psycopg2.extensions.connection
        connection information
    source_raster : str
        source raster name

    Returns
    ----------
    bool: If raster data already in table
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM park_ndvi_stats
            WHERE source_raster = %s
            LIMIT 1
        """, (source_raster,))
        return cur.fetchone() is not None

def insert_zonal_stats(conn: psycopg2.extensions.connection, stats: dict) -> None:
    """
    Adds stats to DB 

    Parameters
    ----------
    conn : psycopg2.extensions.connection
        connection information
    stats : dict
        Stats information with keys: 
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO park_ndvi_stats (
                park_name,
                date,
                year,
                month,
                mean_ndvi,
                std_ndvi,
                min_ndvi,
                max_ndvi,
                valid_pixels,
                source_raster
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            stats["park_name"],
            stats["date"],
            stats["year"],
            stats["month"],
            stats["mean_ndvi"],
            stats["std_ndvi"],
            stats["min_ndvi"],
            stats["max_ndvi"],
            stats["valid_pixels"],
            stats["source_raster"]
        ))
    conn.commit()


# ORCHESTRATION LAYER
def process_file_if_needed(conn: psycopg2.extensions.connection, tif_path: str) -> bool:
    """
    Checks if rater already used in DB, if not computes stats and adds to DB 

    Parameters
    ----------
    conn : psycopg2.extensions.connection
        connection information
    tif_path : str
        path to clipped raster

    Returns
    ----------
    bool: raster uploaded=True, raster skipped=False
    """
    filename = os.path.basename(tif_path)

    if raster_already_loaded(conn, filename):
        logging.info(f"Skipping {filename} (already in DB)")
        return False

    logging.info(f"Processing {filename}...")
    stats = compute_zonal_stats(tif_path)
    insert_zonal_stats(conn, stats)
    logging.info(f"Inserted {filename}")
    return True

def get_files_to_process(single_file: str | None) -> list:
    """
    Translate single_file to a list or get all files in 'processed' folder 

    Parameters
    ----------
    single_file :
        file_name if provided by user
    
    Returns
    ----------
    list: of file paths
    """
    if single_file:
        if not os.path.exists(PROCESSED_DATA_DIR / single_file):
            logging.error(f"ABORTING: {single_file} not found in {PROCESSED_DATA_DIR}")
            raise FileNotFoundError(single_file)
        return [single_file]

    return glob.glob(os.path.join(PROCESSED_DATA_DIR, "*.tif"))

# MAIN
def main():
    """
    Main function call for compute_zonal_stats.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/zonal_stats.log"),
        logging.StreamHandler()
    ]
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=False, help="Path to a single .tif file to process")
    args = parser.parse_args()
    if args.file:
        logging.info(f'COMPUTING ZONAL SATATS for {args.file}')
    else:
        logging.info(f'COMPUTING ZONAL SATATS for all files in {PROCESSED_DATA_DIR}')
    
    files = get_files_to_process(args.file)

    if not files:
        logging.warning("No files found to process")
        return
    
    load_dotenv()
    logging.info(f"Opening connection to {os.getenv("POSTGRES_DB")}...")
    conn = get_db_connection()

    logging.info(f"Processing {len(files)} files...")
    fails = []
    added = []
    skipped = []
    for tif_path in files:
        try:
            uploaded = process_file_if_needed(conn, tif_path)
            if uploaded: added.append(tif_path)
            else: skipped.append(tif_path)
        except:
            logging.warning(f"Failed to commit {tif_path} data to DB")
            fails.append(tif_path)
    
    conn.close()
    logging.info(
        f"\n--- COMPUTE ZONAL STATS Summary ---"
        f"\n{len(added)} rows successfully added "
        f"\n{len(skipped)} files skipped (already in DB)"
        f"\n{len(fails)} files failed"
    )
    logging.info(f'COMPLETED ZONAL SATATS for {len(files)}')


if __name__ == "__main__":
    main()