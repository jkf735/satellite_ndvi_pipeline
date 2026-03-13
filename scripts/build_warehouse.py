"""
build_warehouse.py
Creates or updates and runs tests on a duckdb warehouse.db with dims, facts, and marts based off of data in parks_validated and park_ndvi_stats

Inputs: 
   - Optional arguments for quickstart mode:
        - quickstart: bool
   - parks_validated and park_ndvi_stats exist and populated
Outputs: 
   - warehouse/warehouse.db

Usage:
    Quickstart mode:
        python3 scripts/build_warehouse.py --quickstart
        make warehouse QUICKSTART=True
    Pipeline mode:
        python3 scripts/build_warehouse.py
        make warehouse
"""
import os
import logging
import duckdb
import argparse
import pandas as pd
from db import get_db_connection

from resources.config import WAREHOUSE_DB, MODELS_DIR

logger = logging.getLogger("build_warehouse")

def extract_from_parquet(parquet_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load staging data from local parquet files for quickstart mode.

    Parameters
    ----------
    parquet_dir: str
        local directory with parquet tables
    
    Returns
    ----------
    tuple:
        [parks dataframe, ndvi dataframe]
    """
    parks_df = pd.read_parquet(os.path.join(parquet_dir, "parks_validated.parquet"))
    ndvi_df = pd.read_parquet(os.path.join(parquet_dir, "park_ndvi_stats.parquet"))
    # align columns to match Postgres extract queries
    parks_df = parks_df[["park_code", "park_name"]].rename(columns={"park_name": "unit_name"})
    ndvi_df = ndvi_df[["park_code", "date", "mean_ndvi", "std_ndvi"]]
    return parks_df, ndvi_df

def extract_table(query: str) -> pd.DataFrame:
    """
    Runs a query against Postgres and returns a pandas DataFrame.

    Parameters
    ----------
    query: str
        query to be run
    
    Returns
    ----------
    dataframe:
        result dataframe
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df

def load_to_duckdb(con, table_name: str, df: pd.DataFrame) -> None:
    """
    Loads data from a dataframe into a table in duckdb warehouse

    Parameters
    ----------
    con: DuckDBPyConnection
        duckdb warehouse connection
    table_name: str
        name of table in warehouse
    df: dataframe
        data to be loadeded into table
    """
    logger.info(f"Loading table into DuckDB: {table_name}")
    con.register("temp_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
    con.unregister("temp_df")

def run_sql_models(con) -> None:
    """
    Execute all dims fact and marts

    Parameters
    ----------
    con: DuckDBPyConnection
        duckdb warehouse connection
    """
    logger.info("Running warehouse models...")

    model_layers = [
        "dimensions",
        "facts",
        "marts"
    ]

    for layer in model_layers:
        layer_path = os.path.join(MODELS_DIR, layer)
        for file in sorted(os.listdir(layer_path)):
            if file.endswith(".sql"):
                path = os.path.join(layer_path, file)
                logger.info(f"Executing model: {path}")
                with open(path, "r") as f:
                    sql = f.read()
                con.execute(sql)

def run_tests(con: duckdb.DuckDBPyConnection) -> None:
    """
    Execute warehouse tests (all results expected to be 0)

    Parameters
    ----------
    con: DuckDBPyConnection
        duckdb warehouse connection
    """
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
def main(quickstart: bool = False, parquet_dir: str = "data/quickstart"):
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

    if quickstart:
        logger.info("Bootstrap mode — loading from parquet...")
        parks_df, ndvi_df = extract_from_parquet(parquet_dir)
    else:
        logger.info("Extracting parks_validated...")
        parks_df = extract_table("""
            SELECT park_code, park_name AS unit_name
            FROM parks_validated
        """)
        logger.info("Extracting park_ndvi_stats...")
        ndvi_df = extract_table("""
            SELECT park_code, date, mean_ndvi, std_ndvi
            FROM park_ndvi_stats
        """)

    # load raw operational tables
    logger.info("Loading parks_validated as raw.stg_parks")
    load_to_duckdb(con, "raw.stg_parks", parks_df)
    logger.info("Loading park_ndvi_stats as raw.stg_ndvi")
    load_to_duckdb(con, "raw.stg_ndvi", ndvi_df)

    # run dimensional models
    run_sql_models(con)
    run_tests(con)
    con.close()

    logger.info("Warehouse build complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--quickstart",
        action="store_true",
        help="Build warehouse from local parquet files instead of Postgres."
    )
    parser.add_argument(
        "--parquet_dir",
        type=str,
        default="data/quickstart",
        help="Directory containing parquet files for quickstart mode."
    )
    args = parser.parse_args()
    main(quickstart=args.quickstart, parquet_dir=args.parquet_dir)