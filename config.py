import os
from pathlib import Path

# Project root (this file lives at project root)
PROJECT_ROOT = Path(__file__).resolve().parent

# Data Paths
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
INTERIM_DATA_DIR = DATA_DIR / "interim"
RAW_DATA_DIR = DATA_DIR / "raw"
SENTINEL_PATH = RAW_DATA_DIR / "sentinel_shapefile" / "sentinel_2_index_shapefile.shp"
RAW_BANDS_DIR = RAW_DATA_DIR / "bands"


# Scripts Paths
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
RESOURCE_DIR = SCRIPTS_DIR / "resources"

DB_URI = os.getenv(
    "DB_URI",
    "postgresql://geo_user:geo_pass@localhost:5432/geo"
)