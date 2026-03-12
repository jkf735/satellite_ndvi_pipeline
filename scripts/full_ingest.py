"""
full_ingest.py
Runs tile_ingest->compute_ndvi->clip_to_park->compute_zonal_stats for any number of input park,year,months.
WARNING: if doing a large input ensure you have the file space or set Cleanup to TRUE.

Inputs: Park_name, year, month, cleanup_flag
Outputs: 
   - All clipped ndvi geotiffs in data/processed (raw/interim data cleaned if cleaup=true)
   - All data in park_ndvi_stats

Usage:
    python3 full_ingest.py --parks "yosemite zion" --years "2024 2025" --months "1 2 3 4 5 6 7 8 9 10 11 12" --cleanup true
    make full PARKS="yosemitie zion" YEARS="2024 2025" MONTHS="2 3 4 5 6 7" CLEANUP=True
"""
import os
import shutil
import logging
import argparse
import json
from tile_ingest import main as ingest_main
from compute_ndvi import main as ndvi_main
from clip_to_park import main as clip_main
from compute_zonal_stats import main as zonal_main
from resources.config import RAW_DATA_DIR, INTERIM_DATA_DIR

logger = logging.getLogger("full_ingest")

def cleanup_files(park:str):
    raw_path = RAW_DATA_DIR / park.lower()
    interim_path = INTERIM_DATA_DIR / park.lower()
    if os.path.exists(raw_path) and os.path.isdir(raw_path):
        logging.info(f'DELETING RAW FIELS FOR {park}')
        try:
            shutil.rmtree(raw_path)
            logger.info(f"Directory '{raw_path}' and its contents deleted successfully.")
        except OSError as e:
            logger.warning(f"Error: {raw_path} : {e.strerror}")
    else:
        logger.warning(f"Directory '{raw_path}' not found or is not a directory.")
    if os.path.exists(interim_path) and os.path.isdir(interim_path):
        logging.info(f'DELETING INTERIM FIELS FOR {park}')
        try:
            shutil.rmtree(interim_path)
            logger.info(f"Directory '{interim_path}' and its contents deleted successfully.")
        except OSError as e:
            logger.warning(f"Error: {interim_path} : {e.strerror}")
    else:
        logger.warning(f"Directory '{interim_path}' not found or is not a directory.")

def main(parks=None, years=None, months=None, cleanup=True):
    """
    Main function call for tile_ingest.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/full_ingest.log"),
        logging.StreamHandler()
    ]
    )
    if parks is None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--parks", type=str, nargs='+', required=True)
        parser.add_argument("--years", type=int, nargs='+', required=True)
        parser.add_argument("--months", type=int, nargs='+', required=True)
        parser.add_argument("--cleanup", action="store_true")
        args = parser.parse_args()
        parks, years, months, cleanup = args.parks, args.years, args.months, args.cleanup

    fail_dict = {}
    for park in parks:
        for year in years:
            for month in months:
                logging.info(f'STARTING FULL INGEST FOR {park}, {year}, {month}')
                try:
                    step = 'ingest'
                    ingest_main(park, year, month)
                    step = 'ndvi'
                    ndvi_main(park, year, month)
                    step = 'clip'
                    clip_main(park, year, month)
                except Exception as e:
                    fail_dict[f'{park}-{year}-{month}'] = f"{step} - {e}"
                    continue
            if cleanup:
                logging.info(f'STARTING CLEANUP for {park}')
                cleanup_files(park)   
    logging.warning(f'THE FOLLOWING WERE NOT COPLETED: {json.dumps(fail_dict,indent=4)}')
    logging.info(f'STARTING FULL ZONAL STATS')
    zonal_main(arguments=True)
    logging.info(f'COMPLETED FULL INGEST for {parks}, {years}, {months}')

if __name__ == "__main__":
    main()