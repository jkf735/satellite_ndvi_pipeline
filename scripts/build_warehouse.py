import os
import logging
import duckdb
import pandas as pd
from db import get_db_connection

from resources.config import WAREHOUSE_DB, MODELS_DIR

logger = logging.getLogger("build_warehouse")


def extract_table(query):
    """
    Runs a query against Postgres and returns a pandas DataFrame.
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df


def load_to_duckdb(con, table_name, df):
    logger.info(f"Loading table into DuckDB: {table_name}")
    con.register("temp_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
    con.unregister("temp_df")


def run_sql_models(con):
    logger.info("Running warehouse models...")

    for root, _, files in os.walk(MODELS_DIR):
        for file in sorted(files):
            if file.endswith(".sql"):
                path = os.path.join(root, file)
                logger.info(f"Executing model: {path}")
                with open(path, "r") as f:
                    sql = f.read()
                con.execute(sql)

def run_tests(con):
    logger.info("Running warehouse tests...")
    test_dir = "warehouse/tests"
    for file in sorted(os.listdir(test_dir)):
        if file.endswith(".sql"):
            path = os.path.join(test_dir, file)
            logger.info(f"Running test: {path}")
            with open(path) as f:
                sql = f.read()
            result = con.execute(sql).fetchall()
            if result == [] or result == [(0,)]:
                logger.info(f"Test passed: {file}")
            else:
                raise ValueError(f"Test failed: {file} returned {result}")
            


# Main
def main():
    """
    Main function call for build_warehouse.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/warehouse.log"),
        logging.StreamHandler()
    ]
    )

    logger.info("Connecting to DuckDB...")
    try:
        con = duckdb.connect(WAREHOUSE_DB)
    except:
        logger.error(f"Failed to connect to {WAREHOUSE_DB}")
        return
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    
    # extract operational tables
    logger.info("Extracting parks_raw...")
    parks_df = extract_table("""
        SELECT unit_code AS park_code, unit_name
        FROM parks_raw
    """)

    logger.info("Extracting park_ndvi_stats...")
    ndvi_df = extract_table("""
        SELECT park_code, date, mean_ndvi, std_ndvi
        FROM park_ndvi_stats
    """)

    # load raw operational tables
    logger.info("Loading parks_raw as raw.stg_parks")
    load_to_duckdb(con, "raw.stg_parks", parks_df)
    logger.info("Loading park_ndvi_stats as raw.stg_ndvi")
    load_to_duckdb(con, "raw.stg_ndvi", ndvi_df)


    # run dimensional models
    run_sql_models(con)
    run_tests(con)
    con.close()

    logger.info("Warehouse build complete.")


if __name__ == "__main__":
    main()
    #TODO comment this code
    #TODO marts
    #TODO master script that runs everything for a park, year, month