THIS_FILE := $(lastword $(MAKEFILE_LIST))
LOG_RETENTION_DAYS = 30
LOG_SIZE_LIMIT = 50M
LOG_DIR=logs


.PHONY: up down reset logs psql clean_logs init full ingest_tiles ndvi clip zonal_stats warehouse s3_cog_upload s3_stac_upload s3_stats_export quickstart dashboard titiler

# DOCKER PROCESSES
up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

reset:
	docker compose -f docker/docker-compose.yml down -v
	docker compose -f docker/docker-compose.yml up -d

logs:
	docker compose -f docker/docker-compose.yml logs -f

psql:
	psql postgresql://geo_user:geo_pass@localhost:5432/geo

# LOGGING
clean_logs:
	@[ -n "$(LOG_DIR)" ] || (echo "ERROR: LOG_DIR is not set"; exit 1)
	@[ -n "$(LOG_RETENTION_DAYS)" ] || (echo "ERROR: LOG_RETENTION_DAYS is not set"; exit 1)
	@[ -n "$(LOG_SIZE_LIMIT)" ] || (echo "ERROR: LOG_SIZE_LIMIT is not set"; exit 1)
	@echo "Cleaning logs..."
	@find $(LOG_DIR) -type f -name "*.log" -mtime +$(LOG_RETENTION_DAYS) -delete
	@find $(LOG_DIR) -type f -name "*.log" -size +$(LOG_SIZE_LIMIT) -delete
# SETUP
init:
	@if [ ! -f .env ]; then \
		cp .env.example .env && echo "Created .env from template. Please edit it."; \
	fi
	python3 scripts/init.py
# SCRIPTS
full: clean_logs
	@if [ -z "$(PARKS)" ] || [ -z "$(YEARS)" ] || [ -z "$(MONTHS)" ]; then \
		echo "ERROR: Must provide PARKS, YEARS, and MONTHS and CLEANUP"; \
		echo 'Usage: make full PARKS="yosemite zion" YEARS="2024 2025" MONTHS="2 3 4 5 6 7" CLEANUP=True'; \
	else \
		python3 scripts/full_ingest.py --parks $(PARKS) --years $(YEARS) --months $(MONTHS) $(if $(filter true True 1,$(CLEANUP)),--cleanup,); \
	fi
ingest_tiles: clean_logs
	@if [ -z "$(PARK)" ] || [ -z "$(YEAR)" ] || [ -z "$(MONTH)" ]; then \
		echo "ERROR: Must provide PARK, YEAR, and MONTH"; \
		echo "Usage: make ingest_tiles PARK=Yosemite YEAR=2025 MONTH=11"; \
	else \
		python3 scripts/tile_ingest.py --park $(PARK) --year $(YEAR) --month $(MONTH); \
	fi

ndvi: clean_logs
	@if [ -z "$(PARK)" ] || [ -z "$(YEAR)" ] || [ -z "$(MONTH)" ]; then \
		echo "ERROR: Must provide PARK, YEAR, and MONTH"; \
		echo "Usage: make ndvi PARK=Yosemite YEAR=2025 MONTH=11"; \
	else \
		python3 scripts/compute_ndvi.py --park $(PARK) --year $(YEAR) --month $(MONTH); \
	fi

clip: clean_logs
	@if [ -z "$(PARK)" ] || [ -z "$(YEAR)" ] || [ -z "$(MONTH)" ]; then \
		echo "ERROR: Must provide PARK, YEAR, and MONTH"; \
		echo "Usage: make clip PARK=Yosemite YEAR=2025 MONTH=11"; \
	else \
		python3 scripts/clip_to_park.py --park $(PARK) --year $(YEAR) --month $(MONTH); \
	fi

zonal_stats: clean_logs
	@if [ -z "$(FILE)" ]; then \
		echo "Running zonal stats on all processed files..."; \
		python3 scripts/compute_zonal_stats.py; \
	else \
		echo "Running zonal stats on $(FILE)..."; \
		python3 scripts/compute_zonal_stats.py --file $(FILE); \
	fi

# S3 INTERACTION
s3_cog_upload: clean_logs
	python3 scripts/s3_cog_upload.py $(if $(filter true True 1,$(OVERWRITE)),--overwrite,)

s3_stac_upload: clean_logs
	python3 scripts/s3_stac_upload.py $(if $(filter true True 1,$(OVERWRITE)),--overwrite,)

s3_stats_export:
	python3 scripts/s3_stats_export.py

# WAREHOUSE
warehouse: clean_logs
	python3 scripts/build_warehouse.py $(if $(filter true True 1,$(BOOTSTRAP)),--bootstrap,) $(if $(PARQUET_DIR),--parquet_dir $(PARQUET_DIR),)

# QUICKSTART
quickstart:
	python3 quickstart.py $(if $(filter true True 1,$(OVERWRITE)),--overwrite,)

# DASHBOARD:
dashboard:
	streamlit run dashboard/Overview.py
titiler:
	uvicorn dashboard.titiler_app:app --host localhost --port 8001