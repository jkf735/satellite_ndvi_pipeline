"""
s3_stats_upload.py
Uploads the contents of local Postgres DB tables 'park_ndvi_stats' and 'parks_validated' to S3 bucket as a parquet

Inputs: 
   - park_ndvi_stats and parks_validated must exist in your local Postgres DB
Outputs: 
   - stats/park_ndvi_stats.parquet and stats/parks_validated.parquet updated in S3 bucket 

Usage:
    python3 scripts/s3_stats_upload.py
    make s3_stats_upload
"""
import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from io import BytesIO

from resources.config import DB_URI, S3_BUCKET_NAME, S3_STATS_KEY, S3_PARKS_VALID_KEY


logger = logging.getLogger(__name__)


def export_table_to_s3(table_name: str, s3_key: str, s3_client: boto3.client, engine) -> None:
    """
    Export a Postgres table to parquet and upload to S3.

    Parameters
    ----------
    table_name : str
        Table name being uploaded (park_ndvi_stats or parks_validated)
    s3_key : str
        key for table being uploaded (i.e. "stats/park_ndvi_stats.parquet")
    s3_client : boto3.client
        Boto3 S3 client
    engine :
        Postgres engine
    """
    logger.info(f"Reading {table_name}...")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    logger.info(f"Read {len(df)} rows from {table_name}")

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    logger.info(f"Uploading to s3://{S3_BUCKET_NAME}/{s3_key}...")
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    logger.info(f"Successfully uploaded {table_name} to S3")


def main():
    """
    Main function call for s3_stats_upload.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/s3.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)

    logger.info("Connecting to Postgres...")
    engine = create_engine(DB_URI)
    s3_client = boto3.client("s3")
    
    export_table_to_s3("park_ndvi_stats", S3_STATS_KEY, s3_client, engine)
    export_table_to_s3("parks_validated", S3_PARKS_VALID_KEY, s3_client, engine)


if __name__ == "__main__":
    main()