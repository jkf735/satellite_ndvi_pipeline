import rasterio
import numpy as np
import os
#TODO NOT USING CURRENTLY BUT WILL NEED IF BANDS HAVE DIFFERENT RESOLUTION (CURRENTLY BOTH 10m)
from rasterio.enums import Resampling

from resources.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

def compute_ndvi(red_path:str, nir_path:str, output_path:str)->None:

    #TODO future considerations below:
    """
    # Resolution safety
    if red.shape != nir.shape:
        raise ValueError("Band dimensions mismatch")
    # Reflectance Scaling
    red = red / 10000
    nir = nir / 10000
    # NoData handling
    red = np.where(red == 0, np.nan, red)
    nir = np.where(nir == 0, np.nan, nir)
    # Windowed reading for larger mosaics (currently reading entire raster into memory)
    """

    with rasterio.open(red_path) as red_src:
        # Sentinel bands are single-band so always read the 1st band
        red = red_src.read(1).astype("float32")
        profile = red_src.profile
    
    with rasterio.open(nir_path) as nir_src:
        nir = nir_src.read(1).astype("float32")
    
    # avoid divide by zero errors
    np.seterr(divide='ignore', invalid='ignore')
    # calculate ndvi
    ndvi = (nir-red)/(nir+red)
    # convert any divide-by-0 to np.nan (undefined)
    ndvi = np.where((nir+red)==0, np.nan, ndvi)
    # update our profile for float32 NDVI, single band, lzw compressed
    profile.update(
        driver="GTiff",
        dtype=rasterio.float32,
        count=1,
        compress='lzw'
    )

    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(ndvi.astype(rasterio.float32), 1)
    
    print(f"NDVI written to {output_path}")


if __name__ == "__main__":
    output_path = PROCESSED_DATA_DIR / "2025-11-14_ndvi.tif"
    red_path = RAW_DATA_DIR / "2025-11-14_B04.jp2"
    nir_path = RAW_DATA_DIR / "2025-11-14_B08.jp2"
    compute_ndvi(red_path, nir_path, output_path)