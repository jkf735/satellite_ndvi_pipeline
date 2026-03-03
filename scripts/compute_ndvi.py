import os
import logging
import argparse
import rasterio
import numpy as np

from resources.config import INTERIM_DATA_DIR

logger = logging.getLogger("compute_ndvi")

def find_files(folder, year, month) -> tuple:
    """
    Look for the red (B04) and nir (B08) .tif files in a given folder for a give year, month and return their paths

    Parameters
    ----------
    folder:
        path to interim/{park} folder
    year: int or str
        year being looked at (e.g. 2025)
    month: int or str
        month being looked at (e.g. 11)
        
    Returns
    ----------
    (red_band_path, nir_band_path, output_path): tuple
        B04_band_path, B08_band_path, path_to_new_output
    """
    year = str(year)
    month = str(month)
    if not os.path.isdir(folder):
        logger.warning(f"Input folder not found: {folder}")
        raise FileNotFoundError(f"Input folder not found: {folder}")
    files = sorted(os.listdir(folder))
    red_file = ""
    nir_file = ""
    for file in files:
        split = file.split('_')
        file_year = split[0]
        file_month = split[1]
        file_band = split[3].replace(".tif","")
        if file_year ==  str(year) and file_month == str(month):
            if file_band == "B04": red_file = file
            elif file_band == "B08": nir_file = file

    if red_file == "":
        logger.error(f"B04 (red) file not found in: {folder} for year:{year}, month:{month}")
        raise FileNotFoundError(f"B04 (red) file not found in: {folder} for year:{year}, month:{month}")
    if nir_file == "":
        logger.error(f"B08 (nir) file not found in: {folder} for year:{year}, month:{month}")
        raise FileNotFoundError(f"B08 (nir) file not found in: {folder} for year:{year}, month:{month}")
    output_file = red_file.replace('B04','NDVI').replace('_mosaic','')
    red_path = folder / red_file
    nir_path = folder / nir_file
    output_path = folder / output_file

    logger.info(f"Found input files: red:{red_path}, nir:{nir_path}")
    return red_path, nir_path, output_path

def compute_ndvi_from_tif(red_path: str, nir_path: str, output_path: str) -> bool:
    """
    Compute NDVI from band tif and write GeoTIFF.

    NDVI = (NIR - RED) / (NIR + RED)

    Parameters
    ----------
    red_path : str
        Path to B04 .tif (Red band)
    nir_path : str
        Path to B08 .tif (NIR band)
    output_path : str
        Path where NDVI GeoTIFF will be written

    Returns
    ----------
    bool : If NDVI was computed or not
    """
    if os.path.exists(output_path):
        logger.warning(f'NDVI file already exist at {output_path}. If you want to compute NDVI again, delete the current tif and run again.')
        return False

    if not os.path.exists(red_path):
        logger.error(f"Red band not found: {red_path}")
        raise FileNotFoundError(f"Red band not found: {red_path}")

    if not os.path.exists(nir_path):
        logger.error(f"NIR band not found: {nir_path}")
        raise FileNotFoundError(f"NIR band not found: {nir_path}")

    with rasterio.open(red_path) as red_src, rasterio.open(nir_path) as nir_src:
        # validate alignment
        logger.info("Validating Inputs...")
        if red_src.crs != nir_src.crs:
            logger.error("CRS mismatch between red and NIR mosaics")
            raise ValueError("CRS mismatch between red and NIR mosaics")

        if red_src.transform != nir_src.transform:
            logger.error("Transform mismatch between red and NIR mosaics")
            raise ValueError("Transform mismatch between red and NIR mosaics")

        if red_src.width != nir_src.width or red_src.height != nir_src.height:
            logger.error("Dimension mismatch between red and NIR mosaics")
            raise ValueError("Dimension mismatch between red and NIR mosaics")
        
        

        # read data
        logger.info("Reading Inputs...")
        red = red_src.read(1).astype("float32")
        nir = nir_src.read(1).astype("float32")
        red_nodata = red_src.nodata
        nir_nodata = nir_src.nodata
        mask = None
        if red_nodata is not None and nir_nodata is not None:
            mask = (red == red_nodata) | (nir == nir_nodata)

        # compute NDVI
        logger.info("Computing NDVI...")
        np.seterr(divide="ignore", invalid="ignore")
        ndvi = (nir - red) / (nir + red)
        # mask invalid pixels
        ndvi[(nir + red) == 0] = np.nan
        if mask is not None:
            ndvi[mask] = np.nan

        # run QA:
        logger.info("Running NDVI QA...")
        ndvi_qa(ndvi)

        # prepare output profile
        logger.info("Writting To Output...")
        profile = red_src.profile.copy()
        profile.update(
            driver="GTiff",
            dtype=rasterio.float32,
            count=1,
            compress="lzw",
            nodata=np.nan
        )

    # write output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(ndvi.astype(rasterio.float32), 1)

    logger.info(f"NDVI raster Successfully written to: {output_path}")
    return True

def ndvi_qa(ndvi) -> dict:
    """
    Perform QA on produced NDVI 

    Parameters
    ----------
    ndvi :
        ndvi coverage for full tiles
    Returns
    ----------
    dict : {"min", "max", "mean", "std", "nodata_pct"}
    """
    valid_pixels = ~np.isnan(ndvi)
    total_pixels = ndvi.size
    valid_count = np.count_nonzero(valid_pixels)

    if valid_count == 0:
        logger.error("All NDVI pixels are NaN — check input bands.")
        raise ValueError("All NDVI pixels are NaN — check input bands.")

    ndvi_valid = ndvi[valid_pixels]

    ndvi_min = float(np.nanmin(ndvi_valid))
    ndvi_max = float(np.nanmax(ndvi_valid))
    ndvi_mean = float(np.nanmean(ndvi_valid))
    ndvi_std = float(np.nanstd(ndvi_valid))
    nodata_pct = 100 * (1 - valid_count / total_pixels)
    logger.info(
        f"\n--- NDVI QA Summary ---"
        f"\nMin NDVI:  {ndvi_min:.4f}"
        f"\nMax NDVI:  {ndvi_max:.4f}"
        f"\nMean NDVI: {ndvi_mean:.4f}"
        f"\nStd NDVI:  {ndvi_std:.4f}"
        f"\nNodata %:  {nodata_pct:.2f}%"
        f"\nValid pixels: {valid_count}"
        f"\n------------------------"
    )
    # sanity checks
    if ndvi_min < -1.1 or ndvi_max > 1.1:
        logger.error("NDVI values outside expected range (-1 to 1)")
        raise ValueError("NDVI values outside expected range (-1 to 1)")

    if nodata_pct > 90:
        logger.error("Too many nodata pixels — raster may be invalid")
        raise ValueError("Too many nodata pixels — raster may be invalid")

    return {
        "min": ndvi_min,
        "max": ndvi_max,
        "mean": ndvi_mean,
        "std": ndvi_std,
        "nodata_pct": nodata_pct
    }

def main(park=None, year=None, month=None):
    """
    Main function call for compute_ndvi.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/tile_processing.log"),
        logging.StreamHandler()
    ]
    )
    if park is None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--park", type=str, required=True)
        parser.add_argument("--year", type=int, required=True)
        parser.add_argument("--month", type=int, required=True)
        args = parser.parse_args()
        park, year, month = args.park, args.year, args.month
    input_folder = INTERIM_DATA_DIR / park.lower()
    
    logging.info(f'COMPUTING NDVI FOR {park}, {year}, {month}')
    red_path, nir_path, output_path = find_files(input_folder, year, month)
    complete = compute_ndvi_from_tif(red_path, nir_path, output_path)
    if complete:
        logging.info(f'COMPLETED NDVI COMPUTATION {park}, {year}, {month}')
    else:
        logging.info(f'NDVI GENERATION SPKIPPED: file alread exists for {park}, {year}, {month} at {output_path}')

if __name__ == "__main__":
    main()