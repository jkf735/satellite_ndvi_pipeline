# satellite-ndvi-pipeline

## Overview
This project builds an end-to-end geospatial data pipeline for processing satellite-derived NDVI (Normalized Difference Vegetation Index) data and modeling it in a DuckDB warehouse.
An autonomous pipeline exists that covers fetching raw S3 tiles, producing clipped NDVI rasters ready for analysis and adding NDVI data to the warehouse.

---

## Features

- Fetches Sentinel-2 tiles from AWS S3
- Selects low-cloud, high-coverage imagery automatically
- Mosaics multiple tiles into a seamless composite to cover full park area(as needed)
- Computes NDVI (Normalized Difference Vegetation Index)
- Clips NDVI rasters to park or protected area boundaries
- Adds new, processed park NDVI data to a DuckDB warehouse for analysis
- Logs and caches metadata for reproducibility along the way

---

## Pipeline Overview

```mermaid
flowchart TD
    A["Raw Sentinel-2 Tiles (JP2)"] --> B[Tile Selection / Filtering]
    B --> C{Multiple Tiles?}
    C -- Yes --> D[Mosaic Tiles]
    C -- No --> E
    D --> E[Compute NDVI]
    E --> F[Clip NDVI to Park Boundaries]
    F --> G["Processed NDVI Output (GeoTIFF)"]
    G --> H[DuckDB analytical warehouse]
```

---

## Project Structure

```
satellite-ndvi-pipeline/
│
├── data/
│   ├── raw/                # Downloaded Sentinel-2 JP2 tiles, shapefiles, and boundary.geojsons
│   ├── interim/            # Mosaic and NDVI outputs
│   └── processed/          # Park clipped NDVI outputs
│
├── docker/                   # Docker files
|
├── logs/                   # Log files
|
├── scripts/                # Python scripts for downloading, mosaicking, and processing
|   ├── resources/          # Config files, Cache files, and temporary/interim data
│   ├── build_warehouse.py
│   ├── clip_to_park.py
│   ├── compute_ndvi.py
│   ├── db.py
│   ├── full_ingest.py
│   ├── mosaic_tiles.py
│   ├── init.py
│   └── tile_ingest.py
|
├── sql/                    # SQL functions
|   ├── qa/                 # QA functions (park_validation)
|   └── schema/             # Functions to create tables (parks_raw, parks_ndvi_stats)
|
├── warehouse/              # Warehouse filestructure      
|   ├── models/   
|   |   ├── dimensions/
|   |   ├── facts/  
|   |   └── marts/                 
|   ├── tests/
│   └── warehouse.db        # Warehouse Database     
|
├── .env                    # Must be created on init by user
├── .env.example            # Used to automatically generate a .env file if one is not provided
├── Makefile                # Commands to run pipeline steps with arguments
├── README.md
└── requirements.txt        # Python dependencies
```
Project is setup by default to exist in a containerized WSL environment with data stored locally. This can be configured in config.py (see below)

---

## Getting Started

### Prerequisites

#### WSL2 Configuration
This project stores raster data outside the WSL virtual disk to avoid bloating the `.vhdx` file.
Ensure your `/etc/wsl.conf` contains the following:
```ini
[automount]
enabled = true
root = /mnt/
options = "metadata"
```

After editing, restart WSL:
```powershell
wsl --shutdown
```

#### Data Directory
By default the project expects data to live at `/mnt/d/Code/Projects/satellite-ndvi-pipeline/data/`.
Update `LOCAL_ROOT` in `satellite_ndvi_pipeline/scripts/resources/config.py` to match your local path before running.
If you are not using WSL you can replace all instances of LOCAL_ROOT with PROJECT_ROOT.

#### POSTGRES Database
By default a postgres database will be created in a docker volume when 'make up' is called. The environment details can be found in `satellite_ndvi_pipeline/docker/docker-compose.yml`. You can edit the details of this database here. Make sure any changes are also reflected in your .env file.

### Standard Setup
Update LOCAL_ROOT and PROJECT_ROOT to necessary paths in `scripts/resources/config.py`. If you are using docker, update any database information in `docker/docker-compose.yml`

All Python dependencies are listed in `requirements.txt`. Install them with:

```bash
pip install -r requirements.txt
```

Initialize the docker, postgres environment:

```bash
make up
```
If you are using your own containerized environment, you can skip 'make up' but make sure your database information gets put in the .env file.

Initialize the initial tables in paostgres database:

```bash
make init
```

You can now inspect your database for the following tables: 'parks_ndvi_stats', 'parks_raw', 'parks_repaired', 'parks_validated', 'parks_qa_failures'.

If you used docker you can check your database at the created adminer service: http://localhost:8080/
---

## Usage

Run the pipeline steps in order using the provided Makefile targets:

```bash
1. make up (if using docker)
2. make init (first time only)
3. make full (see full_ingest.py for details)
4. make warehouse
```

Additional scripts can be invoked directly from the `scripts/` directory or via custom Makefile targets for more granular control over individual pipeline steps. Particularly step 3 (make full) can be broken up into 'make tile_ingest', 'make ndvi', 'make clip', and 'make zonal_stats'.

---

## Configuration

Pipeline paths, database connection strings, and constants are managed in `satellite_ndvi_pipeline/scripts/resources/config.py`. This file can be updated to match your local environment or AWS configuration.

Your .env file contains information on your postgres database (this will be created for you from .env.example. Update it with your prefered config. If using docker any changes should also be reflected in docker-compose.yml)

## Warehouse Architecture

The warehouse follows a layered structure inspired by dbt-style projects:

```
raw → dimensions → facts → marts
```

### Schemas

#### `raw`
Contains staging tables loaded directly from extracted data sources.

- `raw.stg_parks`
- `raw.stg_ndvi`

These tables are lightly transformed and serve as the base layer for dimensional modeling.

---

#### `analytics`
Contains core warehouse models including dimensions and fact tables.

##### Dimensions
- `analytics.dim_park`
- `analytics.dim_date`

Dimensions provide descriptive context for analytical queries.

##### Fact Tables
- `analytics.fact_ndvi`

The fact table stores NDVI measurements at the park-date grain.

---

#### `marts`
Contains analytics-ready views designed for specific analytical use cases.

Current marts include:

- `marts.mart_ndvi_monthly` – Monthly NDVI aggregation per park
- `marts.mart_ndvi_trend` – Rolling NDVI trends
- `marts.mart_declining_parks` – Long-term NDVI slope per park
- `marts.mart_ndvi_seasonality` – Seasonal NDVI averages
- `marts.mart_ndvi_anomalies` – Z-score based anomaly detection

These marts are optimized for:
- Exploratory analysis
- Visualization
- Monitoring vegetation health

---

## Current Work

Next steps will extend this pipeline into visualization, validation, and potential cloud deployment.