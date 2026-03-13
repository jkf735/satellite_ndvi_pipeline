"""
quickstart.py
First-time setup for new users. Downloads data from S3 and builds the local DuckDB warehouse.
No Postgres, Docker or .env required.

Usage:
    python3 quickstart.py
"""
import sys
import logging
import argparse
import boto3
import subprocess
from pathlib import Path

from scripts.resources.config import LOGS_DIR, QUICKSTART_DIR, PARQUET_FILES, S3_BUCKET_NAME

logger = logging.getLogger(__name__)



def download_parquets(s3_client: boto3.client, overwrite: bool = False) -> bool:
    """
    Download parquet files from S3 to data/quickstart/

    Parameters
    ----------
    s3_client : boto3.client
        Boto3 S3 client
    overwrite : bool
        If True, Overwrite parquet files that already exist in locally. Default False.
    
    Returns
    ----------
    bool:
        Pass or fail
    """
    QUICKSTART_DIR.mkdir(parents=True, exist_ok=True)

    for s3_key in PARQUET_FILES:
        filename = Path(s3_key).name
        local_path = QUICKSTART_DIR / filename

        if local_path.exists() and not overwrite:
            logger.info(f"Already exists, skipping: {filename}")
            continue

        try:
            logger.info(f"Downloading {filename} from S3...")
            s3_client.download_file(S3_BUCKET_NAME, s3_key, str(local_path))
            logger.info(f"Downloaded: {filename}")
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            return False

    return True


def build_warehouse() -> bool:
    """
    Run build_warehouse.py in quickstart mode

    Returns
    ----------
    Bool:
        Pass or fail
    """
    logger.info("Building DuckDB warehouse from parquet files...")
    result = subprocess.run(
        [sys.executable, "scripts/build_warehouse.py", "--quickstart"],
        capture_output=False
    )
    if result.returncode != 0:
        logger.error("Warehouse build failed.")
        return False
    return True


def main(overwrite: bool = False):
    """
    Main function call for build_warehouse.py
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/quickstart.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)

    logger.info("=== NDVI Pipeline Quickstart ===")

    s3_client = boto3.client("s3", region_name="us-east-1")

    logger.info("Step 1/2 — Downloading data from S3...")
    if not download_parquets(s3_client, overwrite=overwrite):
        logger.error("Quickstart failed at download step.")
        sys.exit(1)

    logger.info("Step 2/2 — Building warehouse...")
    if not build_warehouse():
        logger.error("Quickstart failed at warehouse build step.")
        sys.exit(1)

    logger.info("=== Quickstart complete. Warehouse is ready at warehouse/warehouse.db. ===")
    # TODO logger.info("Run: streamlit run dashboard/app.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download parquet files even if they already exist locally."
    )
    args = parser.parse_args()
    main(overwrite=args.overwrite)