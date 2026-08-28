.PHONY: setup up down logs ch ps restart clean build demo ch-sql ch-init p4 p5 p7_osv p7_changelog dbt-build dbt-test dbt-docs dbt-freshness
-include .env

export
CLICKHOUSE_USER ?= default
CLICKHOUSE_PASSWORD ?= clickhouse123

export DBT_PROJECT_DIR = transform
export DBT_PROFILES_DIR = transform

setup:
	uv sync
	uv run playwright install --with-deps chromium

up:
	docker compose up -d --force-recreate clickhouse-migrate
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

ch-sql:
	docker exec -i repo_radar_clickhouse clickhouse-client \
		--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD) \
		--multiquery < $(FILE)

ch-init:
	@echo "🔧 Инициализация ClickHouse..."
	@for file in infra/clickhouse/init/*.sql; do \
		echo "   Выполняем: $$file"; \
		docker exec -i repo_radar_clickhouse clickhouse-client \
			--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD) \
			--multiquery < $$file; \
	done
	@echo "✅ ClickHouse initialized"

dbt-build:
	uv run dbt build

dbt-test:
	uv run dbt test

dbt-docs:
	uv run dbt docs generate
	uv run dbt docs serve

dbt-freshness:
	uv run dbt source freshness

p4: 
	uv run python scripts/run_p4_pipeline.py

p5:
	uv run python scripts/run_p5_pipeline.py

p7_osv:
	uv run python scripts/run_p7_pipeline.py

p7_changelog: 
	uv run python scripts/run_p7_changelog_pipeline.py

demo: up
	@echo "⏳ Waiting for services to start..."
	@sleep 15
	@echo "✅ Services ready!"
	@echo ""
	@echo "📊 Dagster:        http://localhost:3001"
	@echo "📈 Metabase:       http://localhost:3000"
	@echo "🗄️  MinIO Console:  http://localhost:9001"
	@echo "🔢 ClickHouse:     http://localhost:8123"



