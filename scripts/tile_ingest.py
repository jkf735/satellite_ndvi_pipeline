import os
import json
import logging
import argparse
import itertools
import subprocess
import rasterio
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely.ops import unary_union
from rasterio.merge import merge

from resources.config import RESOURCE_DIR, SENTINEL_PATH, RAW_DATA_DIR, INTERIM_DATA_DIR, DB_URI

logger = logging.getLogger("tile_ingets")
os.environ["GDAL_PAM_ENABLED"] = "NO"

class AWS_INTERFACE:
    """AWS_INTERFACE CLASS"""
    def __init__(self):
        try:
            with open(RESOURCE_DIR / "tile_info_cache.json", 'r', encoding='utf-8') as f:
                self.tile_day_cache = json.load(f)
            logger.info(f"Successfully loadded {RESOURCE_DIR / "tile_info_cache.json"}")
        except:
            self.tile_day_cache = {}
            logger.warning(f"Failed to load {RESOURCE_DIR / "tile_info_cache.json"}. Process will continue but may be slower than usual.")
        self.aws_bucket ="s3://sentinel-s2-l2a/tiles"
        self.aws_args = ["--no-sign-request"]

    def break_down_tile(self, tile:str) -> tuple:
        """
        Break down a tile string into Zone, lat and grid

        Parameters
        ----------
        tile : str
            format: zone-lat_band-grid_square
        
        Returns
        ----------
        (zone, lat_band, grid_square) : tuple
            zone = int, lat_band = str, grid_square = str
        """    
        try:
            zone = int(tile[0:2])
            tile = tile[2:]
        except:
            zone = int(tile[0])
            tile = tile[1:]
        grid_square = tile[-2:]
        lat_band = tile[:-2]
        return zone, lat_band, grid_square
    
    def list_s3(self, prefix:str) -> list:
        """
        Return list of objects/folders under S3 prefix

        Parameters
        ----------
        prefix : str
            file path to be called from aws s3 cli
        
        Returns
        ----------
        list: folders returned from s3 call
        """  
        cmd = ["aws", "s3", "ls", prefix] + self.aws_args
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Error in list_s3: {result.stderr}")
            raise RuntimeError(result.stderr)
        lines = result.stdout.splitlines()
        # Only keep folders (lines ending with '/')
        folders = [l.split()[-1].replace("/", "") for l in lines if l.strip().startswith("PRE")]
        return folders

    def tile_day_passes(self, tile_path:str, day, max_cloud=10, min_coverage=80) -> bool:
        """
        Determine if a tile for given month/day meets cloud and coverage criteria, store results

        Parameters
        ----------
        tile_path : str
            path of s3 cli call, eg: s3://sentinel-s2-l2a/tiles/11/S/KC/2025/11/
        day: int or str
            day of the month being testsed (e.g. 10)
        max_cloud: int
            max cloud coverage allowed for tile to pass (default 10)
        min_coverage: int
            minimum data coverage allowed for tile to pass (default 80)
        
        Returns
        ----------
        bool: tile pass or fail
        """
        day = str(day)  
        # reuse previous result
        if self.tile_day_cache.get(tile_path):
            if day in self.tile_day_cache[tile_path]:
                return self.tile_day_cache[tile_path][day]
        else:
            self.tile_day_cache[tile_path] = {}

        # build the path to tileinfo.json (for simplicity just use index 0)
        tileinfo_path = f"{tile_path}{day}/0/tileInfo.json"
        # use aws cli to get file content
        cmd = ["aws", "s3", "cp", tileinfo_path, "-"] + self.aws_args
        result = subprocess.run(cmd, capture_output=True, text=True)
        # missing file → fail
        if result.returncode != 0:
            self.tile_day_cache[tile_path][day] = False
            logger.info(f'File not found at {cmd}')
            return False

        info = json.loads(result.stdout)
        cloud = info.get("cloudyPixelPercentage", 100)
        data = info.get("dataCoveragePercentage", 0)
        passes = (cloud <= max_cloud and data >= min_coverage)
        self.tile_day_cache[tile_path][day] = passes
        if not passes:
            logger.info(f'Failed Cloud or data coverage check: cloud_coverage={cloud}, data_coverage={data}')
        return passes

    def find_best_tile(self,tile_list:list, year, month, max_cloud=10, min_coverage=80) -> tuple:
        """
        Find first L2A tile for given month/day meeting cloud and coverage criteria

        Parameters
        ----------
        tile_list : list of lists
            list of every combonation of tiles that will cover a whole park (e.g. [['tileA'], ['tileB','tileC'], ...])
        year: int or str
            year being looked at (e.g. 2025)
        month: int or str
            month being looked at (e.g. 11)
        max_cloud: int
            max cloud coverage allowed for tile to pass (default 10)
        min_coverage: int
            minimum data coverage allowed for tile to pass (default 80)
        
        Returns
        ----------
        (tile_combination: list, day: str): tuple
            tile_combination: list of the best tile combination found
            day: day of the month that worked for all tiles
        """
        for combo in tile_list:
            logger.info(f'Checking tile combination {combo}...')
            # get all the candidate days
            candidate_days = []
            for tile in combo:
                zone, lat_band, grid_square = self.break_down_tile(tile)
                prefix = f"{self.aws_bucket}/{zone}/{lat_band}/{grid_square}/{year}/{month}/"
                days = self.list_s3(prefix)
                if candidate_days == []: 
                    candidate_days = days
                else:
                    candidate_days = list(set(candidate_days).intersection(set(days)))
            candidate_days = sorted(candidate_days, key=int)
            for day in candidate_days:
                logger.info(f'Checking day {day}...')
                all_tiles_good = True
                for tile in combo:
                    # if a day fails then break and move to the next day
                    zone, lat_band, grid_square = self.break_down_tile(tile)
                    tile_path = f"{self.aws_bucket}/{zone}/{lat_band}/{grid_square}/{year}/{month}/"
                    if not self.tile_day_passes(tile_path, day, max_cloud, min_coverage):
                        all_tiles_good = False
                        break
                if all_tiles_good:
                    logger.info(f"Found combo {combo} on day {day}")
                    return combo, day
        logger.warning("ABORTING RUN IN 'find_best_tile': Could Not find any tile combination that works for given park.")
        return None, None

    def download_tile_jp2s(self, tile_data:dict, output_dir, bands=['B04','B08']) -> None:
        """
        Download a sentinel tile for each provided band

        Parameters
        ----------
        tile_data : dict
            tile_data = {'tile','year','month','day'}
        output_dir:
            directory to download files to
        bands: list
            list of wanted bands (B04 and B08 by default)        
        """
        try:
            tile = tile_data['tile']
            year = tile_data['year']
            month = tile_data['month']
            day = tile_data['day']
        except KeyError as e:
            logger.error(f"Error in download_tile_jp2s: Missing key: {e}")
            raise KeyError(f"Missing key: {e}")
        zone, lat_band, grid_square = self.break_down_tile(tile)
        # assume always R10m
        base_tile_path = f"{self.aws_bucket}/{zone}/{lat_band}/{grid_square}/{year}/{month}/{day}/0/R10m/"
        for band in bands:
            tile_path = base_tile_path + f"{band}.jp2"
            file_name = f"{tile}_{year}_{month}_{day}_{band}.jp2"
            output = os.path.join(output_dir, file_name)
            cmd = ["aws", "s3", "cp", tile_path, output] + self.aws_args
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Error in download_tile_jp2s: {result.stderr}")
                raise RuntimeError(result.stderr)
            logger.info(f"Successfully downloaded tile to {output}")



def check_if_needed_files_exist(required_tiles:list, park:str, year, month) -> list:
    """
    Check if any combination of needed tiles have already been downloaded

    Parameters
    ----------
    required_tiles : list of lists
            list of every combonation of tiles that will cover a whole park (e.g. [['tileA'], ['tileB','tileC'], ...])
    park: str
        park being looked at (e.g. yosemite)
    year: int or str
            year being looked at (e.g. 2025)
    month: int or str
        month being looked at (e.g. 11)
        
    Returns
    ----------
    list
        list of the tile combination found in the parks directory (or empty list if none exist)
    """
    folder_path = os.path.join(RAW_DATA_DIR, park.lower())
    if not os.path.isdir(folder_path):
        return []
    files = sorted(os.listdir(folder_path))
    found_tiles = []
    for file in files:
        split = file.split('_')
        file_tile = split[0]
        file_year = split[1]
        file_month = split[2]
        if file_year ==  str(year) and file_month == str(month):
            if file_tile not in found_tiles:
                found_tiles.append(file_tile)
    for combo in required_tiles:
        if set(combo).issubset(found_tiles):
            return combo
    return []
        
def find_tiles(park_name:str) -> list:
    """
    Return list of lists of tile combinations that cover the full park area based on the sentinel_shapefile

    Parameters
    ----------
    park_name: str
        park being looked at (e.g. yosemite)
        
    Returns
    ----------
    list of lists
        list of every combonation of tiles that will cover a whole park (e.g. [['tileA'], ['tileB','tileC'], ...])
    """
    # load sentinel shapefile
    tiles_gdf = gpd.read_file(SENTINEL_PATH)
    if tiles_gdf.empty:
        logger.error(f"Error in find_tiles: Issue opening Sentinel Shapefile at {SENTINEL_PATH}")
        raise ValueError(f"Issue opening Sentinel Shapefile at {SENTINEL_PATH}")
    # connect to PostGIS
    engine = create_engine(DB_URI)
    # pull park boundary
    query = f"""
        SELECT geom
        FROM parks_validated
        WHERE park_name ILIKE '{park_name}%'
    """
    park_gdf = gpd.read_postgis(text(query), engine, geom_col="geom")
    if park_gdf.empty:
        logger.warning(f"ABORTING RUN IN 'find_tiles': No {park_name} geometry found in database.")
        raise ValueError(f"No {park_name} geometry found in database.")
    park_gdf = park_gdf.to_crs(tiles_gdf.crs)
    park_geom = park_gdf.geom.iloc[0]   
    # get intersecting tiles
    intersecting_tiles = tiles_gdf[tiles_gdf.intersects(park_geom)].copy()
    # reproject those tiles and find the intersecton area in m^2
    utm_crs = park_gdf.estimate_utm_crs()
    park_proj = park_gdf.to_crs(utm_crs)
    park_proj_geom = park_proj.geometry.iloc[0]
    tiles_proj = intersecting_tiles.to_crs(utm_crs)
    tiles_proj["intersection_geom"] = tiles_proj.geometry.intersection(park_proj_geom)
    tiles_proj["intersection_area"] = tiles_proj["intersection_geom"].area
    # get every combination of full coverage in order from least to most tiles (sub-order by coverage area)
    tiles_proj = tiles_proj.sort_values(by="intersection_area")
    geoms = tiles_proj.geometry.tolist()
    names = tiles_proj["Name"].tolist()
    n = len(geoms)
    list_of_lists = []
    for r in range(1, n+1):
        for combo_indices in itertools.combinations(range(n), r):
            combined = unary_union([geoms[i] for i in combo_indices])
            if combined.contains(park_proj_geom):
                list_of_lists.append([names[i] for i in combo_indices])

    return list_of_lists

def generate_tif(input_files:list, output_folder) -> None:
    """
    Generate a tif file for each band of given input files. If multiple files then they will be moasiced (based on band) from input file list

    Parameters
    ----------
    input_files: list
        list of input file paths
    output_folder:
        directory of output folder
    """
    if len(input_files) == 0:
        logger.error("ABORTING MOSAIC IN 'generate_tif': No input files provided to generate_tif")
        raise ValueError("No input files provided to generate_tif")
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder, exist_ok=True)
    # sort by band into seperate lists
    tile_dict = {}
    for i in input_files:
        band = str(i).split('_')[-1]
        if not tile_dict.get(band): tile_dict[band] = []
        tile_dict[band].append(i)
    for band,tile_list in tile_dict.items():      
        sources = [rasterio.open(p) for p in tile_list]
        # if just 1 file, no mosaic needed
        if len(sources) == 1:
            output_file_name = str(tile_list[0]).split('/')[-1].split('_',1)[1].replace('.jp2','.tif')
            output_path = output_folder / output_file_name
            src = sources[0]
            output_metadata = src.meta.copy()
            output_metadata.update({
                "driver": "GTiff",
                "height": src.height,
                "width": src.width,
                "count": src.count,
                "dtype": src.dtypes[0],
                "crs": src.crs,
                "transform": src.transform,
            })
            try:
                with rasterio.open(output_path, "w", **output_metadata) as dest:
                    dest.write(src.read())
                logger.info(f"NDVI created at {output_path}")
            except:
                logger.warning(f"ABORTING NDVI IN 'generate_tif': Failed to write to {output_path}")
        # if more than 1 then stitch tiles into mosaic before saving
        else:
            output_file_name = str(tile_list[0]).split('/')[-1].split('_',1)[1].replace('.jp2','_mosaic.tif')
            output_path = output_folder / output_file_name
            # validate
            ref = sources[0]
            for src in sources[1:]:
                if src.crs != ref.crs:
                    logger.warning(f"ABORTING MOSAIC IN 'generate_tif': CRS mismatch: {src.name} vs {ref.name}")
                    raise ValueError(f"CRS mismatch: {src.name} vs {ref.name}")
                if src.res != ref.res:
                    logger.warning(f"ABORTING MOSAIC IN 'generate_tif': Resolution mismatch: {src.name} vs {ref.name}")
                    raise ValueError(f"Resolution mismatch: {src.name} vs {ref.name}")
                if src.count != ref.count:
                    logger.warning(f"ABORTING MOSAIC IN 'generate_tif': Band count mismatch: {src.name} vs {ref.name}")
                    raise ValueError(f"Band count mismatch: {src.name} vs {ref.name}")
                if src.dtypes != ref.dtypes:
                    logger.warning(f"ABORTING MOSAIC IN 'generate_tif': Dtype mismatch: {src.name} vs {ref.name}")
                    raise ValueError(f"Dtype mismatch: {src.name} vs {ref.name}")
            # merge
            mosaic, transform = merge(sources)

            output_metadata = ref.meta.copy()
            output_metadata.update({
                "driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": transform,
            })
            try:
                with rasterio.open(output_path, "w", **output_metadata) as dest:
                    dest.write(mosaic)
                logger.info(f"Mosaic created at {output_path}")
            except:
                logger.warning(f"ABORTING MOSAIC IN 'generate_tif': Failed to write to {output_path}")
        for src in sources:
            src.close()


def ingest_tiles(park:str, year, month) -> None:
    """
    Full process to get .jp2 files from Sentinel into designated folders

    Parameters
    ----------
    park: str
        park being looked at (e.g. yosemite)
    year: int or str
            year being looked at (e.g. 2025)
    month: int or str
        month being looked at (e.g. 11)
    """
    park = park.capitalize()
    # check for every possible combination of tiles that could be used to cover park area
    required_tiles = find_tiles(park)
    # check if any of those combo are already saved locally
    combo = check_if_needed_files_exist(required_tiles, park, year, month)
    # if no suitable local files, retreive them using aws s3 cli
    if not combo:
        logger.info(f'No suitable combination of files are currently downloaded for {park} on {year}-{month}')
        logger.info("Searching AWS S3 for suitable tiles...")
        # Init the aws interface and search for tiles that meet our constraints
        aws_interface = AWS_INTERFACE()
        combo, day = aws_interface.find_best_tile(required_tiles,year,month,max_cloud=10, min_coverage=80)
        if not combo or not day:
            return
        output_path = os.path.join(RAW_DATA_DIR, park.lower())
        # download each tile
        for tile in combo:
            tile_data = {
                'tile': tile,
                'year': year,
                'month': month,
                'day': day
            }
            logger.info(f"Attempting to download bands for tile {tile}...")
            aws_interface.download_tile_jp2s(tile_data, output_path)
        try:
            logger.info(f"Updating {RESOURCE_DIR / "tile_info_cache.json"} with new tile info")
            with open(RESOURCE_DIR /"tile_info_cache.json", 'w') as f:
                json.dump(aws_interface.tile_day_cache, f, indent=4)
        except:
            logger.warning(f"Failed to update {RESOURCE_DIR / "tile_info_cache.json"}")
    else:
        logger.info(f'Required tiles ({combo}) exist for {park} on {year}-{month}')

    # mosaic the tiles if they are not already stitched
    interim_path = INTERIM_DATA_DIR / park.lower()
    needed_tiles = combo.copy()
    if os.path.isdir(interim_path):
        files = sorted(os.listdir(interim_path))
        found = False
        for i in files:
            mosaic_year = i.split('_')[0]
            mosaic_month = i.split('_')[1]
            if mosaic_year == str(year) and mosaic_month == str(month):
                found = True
        if found: needed_tiles = []
    else:
        os.makedirs(interim_path, exist_ok=True)
    if not needed_tiles:
        logger.info(f'Required mosaic tiles already exist for {park} on {year}-{month}. If you want to create a new mosaic, delete the current one and run again.')
        return
    # get lists of paths to files
    tile_path = RAW_DATA_DIR / park.lower()
    files = sorted(os.listdir(tile_path))
    file_list = []
    for i in files:
        tile_name = i.split('_')[0]
        tile_year = i.split('_')[1]
        tile_month = i.split('_')[2]
        if tile_name in needed_tiles and tile_year==str(year) and tile_month==str(month):
            file_list.append(tile_path/i)
    if len(combo) == 1:
        logger.info(f"Generating raster for: {combo}")
    else:   
        logger.info(f"Stitching tiles together for tiles: {combo}. Then generating raster")
    generate_tif(file_list, interim_path)


def main(park=None, year=None, month=None):
    """
    Main function call for tile_ingest.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
        logging.FileHandler("logs/tile_ingest.log"),
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
    logging.info(f'STARTING TILE INGEST FOR {park}, {year}, {month}')
    ingest_tiles(park, year, month)
    logging.info(f'COMPLETED TILE INGEST FOR {park}, {year}, {month}')

if __name__ == "__main__":
    main()
    