# RepoRadar

ELT-платформа для аналитики GitHub-репозиториев

## Стек

- **Оркестрация:** Dagster
- **Ingestion:** dlt
- **Storage:** MinIO (bronze), ClickHouse (silver/gold)
- **Трансформации:** dbt
- **Визуализация:** Metabase

## Архитектура
GitHub Archive → dlt → MinIO (bronze) → ClickHouse (silver) → dbt (gold) → Metabase
GitHub API ─────────────────────────────┘
OSV API ────────────────────────────────┘
Playwright ─────────────────────────────┘


## Быстрый старт

```bash
# Поднять инфраструктуру
make up

# Запустить демо
make demo
Порты
Dagster: http://localhost:3001
Metabase: http://localhost:3000
ClickHouse HTTP: http://localhost:8123
MinIO Console: http://localhost:9001
Статус

🚧 В разработке


