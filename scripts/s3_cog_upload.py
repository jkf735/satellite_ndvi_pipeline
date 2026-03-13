"""
s3_cog_upload.py
Uploads contents of data/processed to s3 bucket

Inputs: 
   - overwrite (bool): Optional flag (default = FALSE) to indicate if you want to skip an upload if the file exists or override it.
Outputs: 
   - Contents of data/processed uploaded to thhe s3 buckets '/processed' folder

Usage:
    python3 scripts/s3_cog_upload.py
    make s3_cog_upload OVERWRITE=FALSE (False is default so you can leave this off or set it TRUE)
"""
import boto3
import logging
import argparse
from pathlib import Path

from resources.config import PROCESSED_DATA_DIR, S3_BUCKET_NAME

logger = logging.getLogger(__name__)


def upload_cog_to_s3(local_path: Path, s3_client, skip_existing: bool = True) -> bool:
    """
    Upload a single COG to S3 under processed/{park_name}/filename.

    Parameters
    ----------
    local_path : Path
        Local path to COG file
    s3_client : boto3.client
        Boto3 S3 client
    skip_existing : bool
        If True, skip files that already exist in S3. Default True.

    Returns
    ----------
    bool : success or fail
    """
    park_name = local_path.name.split("_")[0]
    s3_key = f"processed/{park_name}/{local_path.name}"

    if skip_existing:
        try:
            s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            logger.info(f"Already exists, skipping: {s3_key}")
            return True
        except s3_client.exceptions.ClientError:
            pass

    try:
        logger.info(f"Uploading {local_path.name} -> s3://{S3_BUCKET_NAME}/{s3_key}")
        s3_client.upload_file(
            str(local_path),
            S3_BUCKET_NAME,
            s3_key,
            ExtraArgs={"ContentType": "image/tiff"}
        )
        logger.info(f"Successfully uploaded: {s3_key}")
        return True

    except Exception as e:
        logger.error(f"Failed to upload {local_path.name}: {e}", exc_info=True)
        return False


def main(skip_existing: bool = True):
    """
    Main function call for s3_cog_upload.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/s3.log"),
        logging.StreamHandler()
    ]
    )
    tifs = list(PROCESSED_DATA_DIR.glob("*.tif"))
    if not tifs:
        logger.warning(f"No .tif files found in {PROCESSED_DATA_DIR}")
        return

    logger.info(f"Found {len(tifs)} COGs to upload (skip_existing={skip_existing})")

    s3_client = boto3.client("s3")
    success = 0
    failed = []

    for tif in tifs:
        if upload_cog_to_s3(tif, s3_client, skip_existing=skip_existing):
            success += 1
        else:
            failed.append(tif.name)

    logger.info(f"Completed: {success}/{len(tifs)} uploaded successfully")
    if failed:
        logger.warning(f"Failed: {failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-upload files that already exist in S3. Default is to skip existing files."
    )
    args = parser.parse_args()
    main(skip_existing=not args.overwrite)