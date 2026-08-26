# RepoRadar

ELT-платформа для аналитики GitHub-репозиториев

## Стек

- **Оркестрация:** Dagster
- **Ingestion:** dlt
- **Storage:** MinIO (bronze), ClickHouse (silver/gold)
- **Трансформации:** dbt
- **Визуализация:** Metabase

## 🏗️ Архитектура

### Medallion Architecture


GitHub Archive ──┐
GitHub API ──────┤
OSV API ─────────┼──> dlt ──> MinIO (bronze) ──> ClickHouse (silver) ──> dbt (gold) ──> Metabase
Playwright ──────┘                    ↑                                        ↑
└────────────── Dagster ─────────────────┘


**Слои данных:**
- **Bronze (MinIO):** Сырые данные в Parquet — immutable, партиционированные по дате
- **Silver (ClickHouse):** Очищенные таблицы — deduplicated, типизированные, партиционированные
- **Gold (ClickHouse + dbt):** Витрины данных — агрегаты, тренды, метрики для дашбордов

---

## 🚀 Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone <repo-url>
cd repo_radar
```

### 2. Настроить переменные окружения

cp .env.example .env
# Отредактировать .env — добавить GITHUB_TOKEN

### 3. Поднять инфраструктуру

make up

### 4. Установить Python-зависимости

uv sync

### 5. Запустить Dagster

dagster dev -f src/definitions.py

## 🔌 Порты сервисов

Сервис	Порт	URL
Dagster UI	3001	http://localhost:3001
Metabase	3000	http://localhost:3000
MinIO Console	9001	http://localhost:9001
MinIO API	9002	http://localhost:9002
ClickHouse HTTP	8123	http://localhost:8123
ClickHouse TCP	9000	tcp://localhost:9000
PostgreSQL	5433	postgresql://localhost:5433

## 📦 Структура проекта

repo_radar/
├── config/
│   ├── tracked_repos.yml      # ~300 репозиториев для отслеживания
│   └── changelog_sources.yml  # URL для парсинга changelog
├── infra/
│   └── clickhouse/
│       └── init/              # SQL-скрипты инициализации
├── src/
│   ├── definitions.py         # Dagster Definitions
│   ├── assets/                # Dagster assets (bronze/silver/gold)
│   ├── resources/             # Resources (ClickHouse, MinIO, GitHub)
│   └── extractors/            # Логика извлечения данных
├── transform/                 # dbt-проект (будет создан в П6)
├── docker-compose.yml
├── Makefile
└── pyproject.toml

## 🛠️ Технологический стек

Компонент	Технология
Оркестрация	Dagster
Ingestion	dlt
Storage (bronze)	MinIO (S3)
Storage (OLAP)	ClickHouse
Трансформации	dbt
Визуализация	Metabase
Scraping	Playwright
Язык	Python 3.12
Пакет-менеджер	uv


