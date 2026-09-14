.PHONY: setup up down logs ch ps restart clean build dbt-build dbt-test dbt-docs dbt-freshness ci urls
-include .env

export
CLICKHOUSE_USER ?= default
CLICKHOUSE_PASSWORD ?= clickhouse123
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= postgres123

export DBT_PROJECT_DIR = transform
export DBT_PROFILES_DIR = transform

setup:
	uv sync
	uv run playwright install --with-deps chromium

up:
	docker compose up -d --wait minio clickhouse postgres
	docker compose run --rm clickhouse-migrate
	docker compose run --rm postgres-migrate
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

restart:
	docker compose restart

clean:
	docker compose down -v
	rm -rf data/

build:
	docker compose build

ch:
	docker exec -it repo_radar_clickhouse clickhouse-client \
		--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD)

dbt-build:
	uv run dbt build

dbt-test:
	uv run dbt test

dbt-docs:
	uv run dbt docs generate
	uv run dbt docs serve

dbt-freshness:
	uv run dbt source freshness

ci:
	uv run ruff check src tests
	PYTHONPATH=src uv run pytest -q
	cd transform && DBT_PROJECT_DIR=. DBT_PROFILES_DIR=. \
	  uv run --project .. dbt deps --profiles-dir . && \
	  DBT_PROJECT_DIR=. DBT_PROFILES_DIR=. \
	  uv run --project .. dbt parse --project-dir . --profiles-dir .

urls:
	@echo "======================================================================"
	@echo "✅ RepoRadar готов. Сервисы и порты:"
	@echo "======================================================================"
	@echo "📈 Metabase:        http://localhost:3000"
	@echo "📊 Dagster:         http://localhost:3001"
	@echo "🗄️  MinIO Console:   http://localhost:9001"
	@echo "🔌 MinIO API (S3):  http://localhost:9002"
	@echo "🔢 ClickHouse HTTP: http://localhost:8123"
	@echo "🐘 Postgres:        localhost:5433"
	@echo "======================================================================"



