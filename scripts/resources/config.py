import os
from pathlib import Path

# Project root (this file lives at project root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_ROOT = Path("/mnt/d/Code/Projects/satellite-ndvi-pipeline")

# Data Paths
DATA_DIR = LOCAL_ROOT / "data"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
INTERIM_DATA_DIR = DATA_DIR / "interim"
RAW_DATA_DIR = DATA_DIR / "raw"
SENTINEL_PATH = RAW_DATA_DIR / "sentinel_shapefile" / "sentinel_2_index_shapefile.shp"

# Log Paths
LOGS_DIR = PROJECT_ROOT / "logs"

# Scripts Paths
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
RESOURCE_DIR = SCRIPTS_DIR / "resources"

# Warehouse Paths
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
WAREHOUSE_DB = WAREHOUSE_DIR / "warehouse.db"
MODELS_DIR = WAREHOUSE_DIR / "models"
MARTS_DIR = MODELS_DIR / "marts"
DIMENSIONS_DIR = MODELS_DIR / "dimensions"
FACTS_DIR = MODELS_DIR / "facts"

DB_URI = os.getenv(
    "DB_URI",
    "postgresql://geo_user:geo_pass@localhost:5432/geo"
)

# S3
S3_BUCKET_NAME = "satellite-ndvi-pipeline"
PROCESSED_PREFIX = "processed/"
STAC_PREFIX = "stac/"
CATALOG_ID = "ndvi-pipeline"
COLLECTION_ID = "ndvi_cog"
S3_STATS_KEY = "stats/park_ndvi_stats.parquet"
S3_PARKS_VALID_KEY = "stats/parks_validated.parquet"

# QUICKSTART
QUICKSTART_DIR = PROJECT_ROOT / "data" / "quickstart"
PARQUET_FILES = [
    S3_STATS_KEY,
    S3_PARKS_VALID_KEY
]