import json
import logging
import boto3
import rasterio
import argparse
from pathlib import Path
from datetime import datetime, timezone
from rasterio.crs import CRS
from rasterio.warp import transform_bounds

from resources.config import S3_BUCKET_NAME, PROCESSED_PREFIX, STAC_PREFIX, CATALOG_ID, COLLECTION_ID

logger = logging.getLogger(__name__)

# S3 HELPERS
def list_cogs_on_s3(s3_client) -> list[str]:
    """Return all .tif keys under processed/ in the bucket"""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=PROCESSED_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".tif"):
                keys.append(obj["Key"])
    logger.info(f"Found {len(keys)} COGs on S3")
    return keys

def read_cog_metadata(s3_key: str) -> dict:
    """
    Read bbox and datetime from COG header without downloading full file
    Uses a range request to read just the GeoTIFF header
    """
    s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
    # using private bucket with locally saved credentials
    with rasterio.open(s3_url) as src:
        bounds = transform_bounds(
            src.crs,
            CRS.from_epsg(4326),
            *src.bounds
        )
    return {
        "bbox": list(bounds),  # [west, south, east, north]
        "s3_key": s3_key
    }

def upload_json_to_s3(data: dict, s3_key: str, s3_client) -> None:
    """Upload a dict as JSON to S3"""
    body = json.dumps(data, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=body,
        ContentType="application/json"
    )
    logger.info(f"Uploaded: s3://{S3_BUCKET_NAME}/{s3_key}")


# FILENAME PARSING
def parse_filename(filename: str) -> dict:
    """
    Parse park, year, month, day from filename
    e.g. yosemite_2023_6_15_NDVI.tif -> {park, year, month, day}
    """
    parts = filename.replace("_NDVI.tif", "").split("_")
    # handle multi-word park names if needed (e.g. park names with underscores)
    day = int(parts[-1])
    month = int(parts[-2])
    year = int(parts[-3])
    park = "_".join(parts[:-3])
    return {"park": park, "year": year, "month": month, "day": day}


# STAC BUILDERS
def build_stac_item(s3_key: str, bbox: list, parsed: dict) -> dict:
    """Build a STAC item dict for a single COG"""
    item_id = Path(s3_key).stem
    dt = datetime(
        parsed["year"], parsed["month"], parsed["day"],
        tzinfo=timezone.utc
    ).isoformat()

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": item_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [bbox[0], bbox[1]],
                [bbox[2], bbox[1]],
                [bbox[2], bbox[3]],
                [bbox[0], bbox[3]],
                [bbox[0], bbox[1]]
            ]]
        },
        "bbox": bbox,
        "properties": {
            "datetime": dt,
            "park": parsed["park"],
            "year": parsed["year"],
            "month": parsed["month"],
            "day": parsed["day"]
        },
        "links": [
            {"rel": "self", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}items/{item_id}.json"},
            {"rel": "collection", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}collections/{COLLECTION_ID}.json"},
            {"rel": "root", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}catalog.json"}
        ],
        "assets": {
            "ndvi": {
                "href": f"s3://{S3_BUCKET_NAME}/{s3_key}",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": f"NDVI COG - {parsed['park']} {parsed['year']}-{parsed['month']:02d}-{parsed['day']:02d}",
                "roles": ["data"]
            }
        },
        "collection": COLLECTION_ID
    }

def build_collection(items: list[dict]) -> dict:
    """Build STAC collection from all items"""
    # derive spatial extent from all item bboxes
    all_bboxes = [item["bbox"] for item in items]
    west = min(b[0] for b in all_bboxes)
    south = min(b[1] for b in all_bboxes)
    east = max(b[2] for b in all_bboxes)
    north = max(b[3] for b in all_bboxes)

    # derive temporal extent
    datetimes = [item["properties"]["datetime"] for item in items]
    start_dt = min(datetimes)
    end_dt = max(datetimes)

    return {
        "type": "Collection",
        "id": COLLECTION_ID,
        "stac_version": "1.0.0",
        "description": "Sentinel-2 derived NDVI Cloud Optimized GeoTIFFs for US National Parks",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[west, south, east, north]]},
            "temporal": {"interval": [[start_dt, end_dt]]}
        },
        "links": [
            {"rel": "self", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}collections/{COLLECTION_ID}.json"},
            {"rel": "root", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}catalog.json"},
            {"rel": "items", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}items/"}
        ],
        "summaries": {
            "parks": list(set(item["properties"]["park"] for item in items))
        }
    }

def build_catalog(item_ids: list[str]) -> dict:
    """Build root STAC catalog"""
    return {
        "type": "Catalog",
        "id": CATALOG_ID,
        "stac_version": "1.0.0",
        "description": "NDVI pipeline STAC catalog for Sentinel-2 National Park data",
        "links": [
            {"rel": "self", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}catalog.json"},
            {"rel": "child", "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}collections/{COLLECTION_ID}.json"}
        ] + [
            {
                "rel": "item",
                "href": f"s3://{S3_BUCKET_NAME}/{STAC_PREFIX}items/{item_id}.json"
            }
            for item_id in item_ids
        ]
    }


# MAIN
def main(skip_existing: bool = True):
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

    # get existing STAC item keys if skipping
    existing_items = set()
    if skip_existing:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=f"{STAC_PREFIX}items/"):
            for obj in page.get("Contents", []):
                existing_items.add(Path(obj["Key"]).stem)
        logger.info(f"Found {len(existing_items)} existing STAC items")

    cog_keys = list_cogs_on_s3(s3_client)
    all_items = []
    failed = []

    for s3_key in cog_keys:
        filename = Path(s3_key).name
        item_id = Path(s3_key).stem
        if skip_existing and item_id in existing_items:
            logger.info(f"Skipping existing STAC item: {item_id}")
            # load existing item for catalog/collection rebuild
            try:
                existing = s3_client.get_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=f"{STAC_PREFIX}items/{item_id}.json"
                )
                all_items.append(json.loads(existing["Body"].read()))
            except Exception:
                pass

            continue

        try:
            parsed = parse_filename(filename)
            metadata = read_cog_metadata(s3_key)
            item = build_stac_item(s3_key, metadata["bbox"], parsed)

            # upload individual item
            upload_json_to_s3(item, f"{STAC_PREFIX}items/{item_id}.json", s3_client)
            all_items.append(item)

        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}", exc_info=True)
            failed.append(filename)

    if not all_items:
        logger.warning("No items to build catalog from")
        return

    # rebuild collection and catalog from all items
    collection = build_collection(all_items)
    catalog = build_catalog([item["id"] for item in all_items])

    upload_json_to_s3(collection, f"{STAC_PREFIX}collections/{COLLECTION_ID}.json", s3_client)
    upload_json_to_s3(catalog, f"{STAC_PREFIX}catalog.json", s3_client)

    logger.info(f"STAC catalog built with {len(all_items)} items")
    if failed:
        logger.warning(f"Failed files: {failed}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Regenerate all STAC items even if they already exist. Default skips existing."
    )
    args = parser.parse_args()
    main(skip_existing=not args.overwrite)