.PHONY: up down logs ch ps restart clean build demo ch-sql ch-init p4

CLICKHOUSE_USER ?= default
CLICKHOUSE_PASSWORD ?= clickhouse123

up:
	docker compose up -d --force-recreate --no-deps clickhouse-migrate
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

p4: 
	uv run python scripts/run_p4_pipeline.py

demo: up
	@echo "⏳ Waiting for services to start..."
	@sleep 15
	@echo "✅ Services ready!"
	@echo ""
	@echo "📊 Dagster:        http://localhost:3001"
	@echo "📈 Metabase:       http://localhost:3000"
	@echo "🗄️  MinIO Console:  http://localhost:9001"
	@echo "🔢 ClickHouse:     http://localhost:8123"



