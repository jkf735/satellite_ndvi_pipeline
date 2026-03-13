"""
s3_stats_upload.py
Uploads the contents of local Postgres DB table 'park_ndvi_stats' to S3 bucket as a parquet

Inputs: 
   - park_ndvi_stats must exist in your local Postgres DB
Outputs: 
   - stats/park_ndvi_stats.parquet updated in S3 bucket 

Usage:
    python3 scripts/s3_stats_upload.py
    make s3_stats_upload
"""
import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO

from resources.config import DB_URI, S3_BUCKET_NAME, S3_STATS_KEY

logger = logging.getLogger(__name__)




def export_stats_to_s3(s3_client) -> None:
    """Export park_ndvi_stats from Postgres to parquet and upload to S3."""
    logger.info("Connecting to Postgres...")
    engine = create_engine(DB_URI)

    logger.info("Reading park_ndvi_stats...")
    df = pd.read_sql("SELECT * FROM park_ndvi_stats", engine)
    logger.info(f"Read {len(df)} rows from park_ndvi_stats")

    # write parquet to in-memory buffer (no temp file needed)
    logger.info("Converting to parquet...")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    logger.info(f"Uploading to s3://{S3_BUCKET_NAME}/{S3_STATS_KEY}...")
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_STATS_KEY,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    logger.info(f"Successfully uploaded {len(df)} rows to S3")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/s3.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)

    s3_client = boto3.client("s3")
    export_stats_to_s3(s3_client)


if __name__ == "__main__":
    main()