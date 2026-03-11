"""
init.py
Initialize file structures and initial sql tables, ingest spatial data, and run QA validation.

Inputs: None
Outputs: 
   - parks_raw ingested from data/raw/nps_boundary.geojson
   - QA validation run and metrics printed

Usage:
    python3 scripts/init.py
    make init
"""
import os
import logging
import subprocess
from glob import glob
from datetime import datetime
from dotenv import load_dotenv
from db import get_db_connection
from resources.config import RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR, RESOURCE_DIR, WAREHOUSE_DIR, MODELS_DIR

load_dotenv()

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

# -------------------------
# Env check
# -------------------------
def check_env():
    required_env_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT"]
    missing = [v for v in required_env_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required env vars: {', '.join(missing)}. Please edit your .env file.")
        raise SystemExit(1)

# -------------------------
# Directories
# -------------------------
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
# Ingest
# -------------------------
def ingest_table(file: str = RAW_DATA_DIR / "nps_boundary.geojson", table: str = "parks_raw"):
    """Ingest a spatial file into PostGIS via ogr2ogr"""
    pg_dsn = (
        f"PG:host={os.getenv('POSTGRES_HOST')} port={os.getenv('POSTGRES_PORT')} "
        f"dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} "
        f"password={os.getenv('POSTGRES_PASSWORD')}"
    )

    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        pg_dsn,
        str(file),
        "-nln", table,
        "-nlt", "MULTIPOLYGON",
        "-t_srs", "EPSG:4326",
        "-overwrite",
    ]

    logger.info(f"===== INGEST START {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")

    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info(line)

    logger.info(f"===== INGEST END {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")

    if result.returncode != 0:
        raise RuntimeError(f"ogr2ogr failed (exit {result.returncode}). See logs/setup.log for details.")

    logger.info(f"Ingested '{file}' → table '{table}' successfully.")

# -------------------------
# QA
# -------------------------
def qa_table(conn):
    """Run parks QA validation SQL and print a metrics summary"""
    qa_file = os.path.join("sql", "qa", "01_parks_validation.sql")
    qa_metrics = [
        ("total_raw",  "SELECT COUNT(*) AS total_raw FROM parks_raw;"),
        ("repaired",   "SELECT COUNT(*) AS repaired FROM parks_repaired WHERE was_valid = FALSE;"),
        ("validated",  "SELECT COUNT(*) AS validated FROM parks_validated;"),
        ("failures",   "SELECT COUNT(*) AS failures FROM parks_qa_failures;"),
    ]

    logger.info(f"===== QA START {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    logger.info(f"Running: {qa_file}")

    with open(qa_file, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"Executed: {qa_file}")

    # Metrics summary
    logger.info("QA Metrics Summary:")
    print("\nQA Metrics Summary:")
    print("-" * 30)
    with conn.cursor() as cur:
        for label, query in qa_metrics:
            cur.execute(query)
            value = cur.fetchone()[0]
            line = f"  {label}: {value}"
            print(line)
            logger.info(line)
    print("-" * 30)

    logger.info(f"===== QA END {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    logger.info("QA complete. Log saved to logs/setup.log")

# -------------------------
# Main
# -------------------------
def main():
    """
    Main function call for init.py
    """
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/setup.log"),
            logging.StreamHandler()
        ]
    )

    check_env()
    ensure_directories()

    conn = get_db_connection()
    try:
        run_all_sql(conn)
        logger.info("Database initialization complete!")

        logger.info("Starting table ingest...")
        ingest_table()

        logger.info("Starting QA validation...")
        qa_table(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()