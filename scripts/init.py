"""
init.py
Initialize file structures and inital sql tables.

Inputs: None
Outputs: 
   - empty parks_raw and park_ndvi_stats table created

Usage:
    python3 init.py --park yosemite --year 2025 --month 11
    make init
"""
import os
import logging
from glob import glob
from db import get_db_connection
from resources.config import RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR, RESOURCE_DIR, WAREHOUSE_DIR, MODELS_DIR

# -------------------------
# Logging
# -------------------------
logger = logging.getLogger("init")

# -------------------------
# Directories
# -------------------------
REQUIRED_FOLDERS = [
    RAW_DATA_DIR,
    INTERIM_DATA_DIR,
    PROCESSED_DATA_DIR,
    LOGS_DIR,
    RESOURCE_DIR,
    WAREHOUSE_DIR,
    MODELS_DIR
]

def ensure_directories():
    for folder in REQUIRED_FOLDERS:
        os.makedirs(folder, exist_ok=True)
        logger.info(f"Ensured directory exists: {folder}")

# -------------------------
# SQL execution
# -------------------------
def run_sql_file(conn, sql_file_path):
    """Run a single SQL file"""
    logger.info(f"Running SQL: {sql_file_path}")
    with open(sql_file_path, "r") as f:
        sql_content = f.read()
    with conn.cursor() as cur:
        cur.execute(sql_content)
    conn.commit()
    logger.info(f"Finished SQL: {sql_file_path}")


def run_all_sql(conn):
    """Run extensions first, then schema files"""
    # 1️⃣ Enable extensions (from docker init folder)
    extensions_file = os.path.join("docker", "postgis", "init", "01_enable_extensions.sql")
    if os.path.exists(extensions_file):
        run_sql_file(conn, extensions_file)
    else:
        logger.warning(f"Extensions file not found: {extensions_file}")

    # 2️⃣ Run schema files in order
    schema_folder = os.path.join("sql", "schema")
    schema_files = sorted(glob(os.path.join(schema_folder, "*.sql")))
    for sql_file in schema_files:
        run_sql_file(conn, sql_file)

# -------------------------
# Main
# -------------------------
def main():
    """
    Main function call for init.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/setup.log"),
        logging.StreamHandler()
    ]
    )
    ensure_directories()
    conn = get_db_connection()
    try:
        run_all_sql(conn)
        logger.info("Database initialization complete!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()