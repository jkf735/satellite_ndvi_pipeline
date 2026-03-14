import duckdb
import pandas as pd
from pathlib import Path

WAREHOUSE_PATH = Path(__file__).parent.parent / "warehouse" / "warehouse.db"


def get_connection():
    return duckdb.connect(str(WAREHOUSE_PATH), read_only=True)


def query(sql: str) -> pd.DataFrame:
    con = get_connection()
    try:
        return con.execute(sql).df()
    finally:
        con.close()