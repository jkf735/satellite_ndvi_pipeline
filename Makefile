.PHONY: up down reset logs psql

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

LOG_DIR=logs
LOG_FILE =$(LOG_DIR)/ingest.log

DB_HOST=localhost
DB_PORT=5432
DB_NAME=geo
DB_USER=geo_user
DB_PASS=geo_pass

INGEST_FILE ?= data/raw/nps_boundary.geojson
TABLE ?= parks_raw

ingest:
	@mkdir -p $(LOG_DIR)
	@echo "===== INGEST START $$(date) =====" | tee -a $(LOG_FILE)
	@ogr2ogr \
	-f "PostgreSQL" \
	PG:"host=$(DB_HOST) port=$(DB_PORT) dbname=$(DB_NAME) user=$(DB_USER) password=$(DB_PASS)" \
	$(INGEST_FILE) \
	-nln $(TABLE) \
	-nlt MULTIPOLYGON \
	-t_srs EPSG:4326 \
	-overwrite 2>&1 | tee -a $(LOG_FILE)
	@echo "===== INGEST END $$(date) =====" | tee -a $(LOG_FILE)