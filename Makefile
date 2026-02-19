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