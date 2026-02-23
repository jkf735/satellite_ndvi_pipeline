DB_HOST=localhost
DB_PORT=5432
DB_NAME=geo
DB_USER=geo_user
DB_PASS=geo_pass

LOG_RETENTION_DAYS = 30
LOG_DIR=logs
INGEST_LOG_FILE=$(LOG_DIR)/ingest.log
QA_LOG_FILE=$(LOG_DIR)/qa_$(shell date +%Y%m%d_%H%M%S).log


.PHONY: up down reset logs psql ingest qa

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

clean_logs:
	@echo "Removing logs older than $(LOG_RETENTION_DAYS) days..."
	@find $(LOG_DIR) -type f -name "*.log" -mtime +$(LOG_RETENTION_DAYS) -delete

INGEST_FILE ?= data/raw/nps_boundary.geojson
INGEST_TABLE ?= parks_raw
ingest: clean_logs
	@mkdir -p $(LOG_DIR)
	@echo "===== INGEST START $$(date) =====" | tee -a $(INGEST_LOG_FILE)
	@ogr2ogr \
	-f "PostgreSQL" \
	PG:"host=$(DB_HOST) port=$(DB_PORT) dbname=$(DB_NAME) user=$(DB_USER) password=$(DB_PASS)" \
	$(INGEST_FILE) \
	-nln $(INGEST_TABLE) \
	-nlt MULTIPOLYGON \
	-t_srs EPSG:4326 \
	-overwrite 2>&1 | tee -a $(INGEST_LOG_FILE)
	@echo "===== INGEST END $$(date) =====" | tee -a $(INGEST_LOG_FILE)

qa: clean_logs
	@mkdir -p $(LOG_DIR)
	@echo "===== QA START $$(date) =====" | tee -a $(QA_LOG_FILE)
	-docker exec -i geo_postgis psql -U geo_user -d geo < sql/qa/01_parks_validation.sql >> $(QA_LOG_FILE) 2>&1
	@echo "QA complete. Log saved to $(QA_LOG_FILE)"
	@echo ""
	@echo "QA Metrics Summary:"
	# Run metrics queries and output to console
	docker exec -i geo_postgis psql -U geo_user -d geo -c "SELECT COUNT(*) AS total_raw FROM parks_raw;"
	docker exec -i geo_postgis psql -U geo_user -d geo -c "SELECT COUNT(*) AS repaired FROM parks_repaired WHERE was_valid = FALSE;"
	docker exec -i geo_postgis psql -U geo_user -d geo -c "SELECT COUNT(*) AS validated FROM parks_validated;"
	docker exec -i geo_postgis psql -U geo_user -d geo -c "SELECT COUNT(*) AS failures FROM parks_qa_failures;"
	@echo "===== QA END $$(date) =====" | tee -a $(QA_LOG_FILE)