# RepoRadar

ELT-платформа для аналитики GitHub-репозиториев: активность (GH Archive), метаданные (GraphQL), уязвимости (OSV), changelogs (Playwright) → MinIO → ClickHouse → dbt → Metabase.

---

## Стек

| Слой | Технология |
|------|------------|
| Оркестрация | Dagster |
| Ingestion / клиенты | httpx, dlt (GraphQL source), Playwright |
| Bronze | MinIO (Parquet, Hive-пути) |
| Silver | ClickHouse (S3Queue + Materialized Views) |
| Gold | dbt-clickhouse |
| UI | Metabase |
| Meta DB | PostgreSQL (Dagster + Metabase) |
| Python | 3.12+, пакет-менеджер **uv** |

---

## Архитектура (medallion)

```
GitHub Archive ──┐
GitHub GraphQL ──┼──> extractors ──> MinIO (bronze/*.parquet)
OSV.dev ─────────┤                      │
Playwright ──────┘                      ▼
                              ClickHouse S3Queue + MV
                                        │
                                        ▼
                                   silver_* (CH)
                                        │
                                        ▼
                              dbt (stg_* → gold_*)
                                        │
                                        ▼
                                     Metabase
```

- **Bronze (MinIO):** сырые/нормализованные parquet (`gharchive/`, `github_repos/`, `osv/`, `changelogs/`).
- **Silver (ClickHouse):** `silver_github_events`, `silver_repos`, `silver_advisories`, `silver_changelogs` — наполняются S3Queue, не Python INSERT (кроме fallback часа GH Archive).
- **Gold (dbt):** `gold_repo_daily`, `gold_language_trends`, `gold_rising_repos`, `gold_cve_exposure`.

Оркестрация: Dagster assets `bronze_*` → барьер `silver_*_ready` → `dbt_build`.

---

## Требования

- Docker + Docker Compose
- [uv](https://docs.astral.sh/uv/)
- GitHub Personal Access Token (для GraphQL; public metadata достаточно)

---

## Быстрый старт

### 1. Клонировать и настроить env

```bash
git clone <repo-url>
cd repo_radar
cp .env.example .env
# Обязательно: GITHUB_TOKEN=ghp_...
```

### 2. Python-зависимости

```bash
make setup
# uv sync + Playwright Chromium (для changelogs)
```

### 3. Инфраструктура

```bash
make up
# MinIO, ClickHouse (+ migrate), Postgres (+ migrate), Dagster webserver/daemon
make urls
```

### 6. Прогнать пайплайн (кратко)

В Dagster:

1. Job **`repos_osv_job`** — GraphQL repos + OSV → silver.
2. Job **`gharchive_job`** — почасовые партиции GH Archive (см. `GHARCHIVE_START_DATE`).
3. Job **`changelogs_job`** — Playwright changelogs.
4. Asset / job с **`dbt_build`** — gold-таблицы.

Затем в Metabase подключить ClickHouse (`host=clickhouse` из контейнера или `localhost:8123` с хоста) и собрать дашборды по [`docs/METABASE_DASHBOARDS.md`](docs/METABASE_DASHBOARDS.md).

---

## Порты

| Сервис | Порт | URL |
|--------|------|-----|
| Metabase | 3000 | http://localhost:3000 |
| Dagster | 3001 | http://localhost:3001 |
| MinIO Console | 9001 | http://localhost:9001 |
| MinIO API (S3) | 9002 | http://localhost:9002 |
| ClickHouse HTTP | 8123 | http://localhost:8123 |
| ClickHouse native | 9000 | tcp://localhost:9000 |
| PostgreSQL | 5433 | localhost:5433 |

MinIO / CH / Postgres дефолтные логины — в `.env.example`.

---

## Переменные окружения

См. `.env.example`:

| Переменная | Назначение |
|------------|------------|
| `GITHUB_TOKEN` | GitHub GraphQL |
| `MINIO_*` | Bronze S3 |
| `CLICKHOUSE_*` | Silver/Gold OLAP |
| `POSTGRES_*` | Dagster + Metabase DB |
| `GHARCHIVE_START_DATE` | Старт hourly-партиций (`YYYY-MM-DD` или `YYYY-MM-DD-HH:MM`) |
| `SLACK_WEBHOOK_URL` | Опционально: алерты runs |
| `DAGSTER_WEBSERVER_URL` | Ссылки в Slack |

---

## Структура репозитория

```
repo_radar/
├── config/                    # YAML seed (tracked repos, changelog sources)
├── docs/ 
│   └── METABASE_DASHBOARDS.md # SQL и настройки карточек Metabase
├── infra/
│   ├── clickhouse/init/       # DDL silver + S3Queue
│   ├── clickhouse/config.d/
│   ├── postgres/init/         # БД dagster, metabase
│   └── dagster/dagster.yaml
├── src/
│   ├── definitions.py         # Dagster Definitions
│   ├── jobs.py                # Jobs + schedules
│   ├── assets/                # bronze / silver_ready / dbt / optimize
│   ├── checks/                # asset checks на silver_*_ready
│   ├── config/                # paths + YAML loaders (Python)
│   ├── extractors/            # gharchive, github, osv, changelog + bronze helpers
│   ├── resources/             # ClickHouse, MinIO, GitHub, Slack
│   ├── sensors/               # Slack on success/failure
│   └── utils/                 # retry, datetime, s3queue wait, formatting
├── transform/                 # dbt project (stg_*, gold_*)
├── tests/
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```

---

## Источники данных

| Источник | Что даёт | Bronze path (пример) | Silver |
|----------|----------|----------------------|--------|
| GH Archive | События по seed-репо | `gharchive/dt=…/hour=…/events.parquet` | `silver_github_events` |
| GitHub GraphQL | Метаданные репо | `github_repos/dt=…/repos_*.parquet` | `silver_repos` |
| OSV.dev | Advisories по ecosystem/package | `osv/dt=…/advisories_*.parquet` | `silver_advisories` |
| Playwright | Версии из changelog HTML | `changelogs/dt=…/changelogs_*.parquet` | `silver_changelogs` |

Seed репозиториев: `config/tracked_repos.yml` (~300 шт.).  
Источники changelog: `config/changelog_sources.yml`.

---

## Dagster: jobs

| Job | Назначение |
|-----|------------|
| `gharchive_job` | Hourly partitions: download → filter → bronze → silver ready |
| `repos_osv_job` | GraphQL snapshot + OSV advisories |
| `changelogs_job` | Playwright scrape + checkpoint |
| `full_pipeline_job` | Снимки + dbt (без gharchive partitions / optimize) |

Schedules — в `src/jobs.py`. Asset checks — `src/checks/silver_checks.py` (row count, freshness, …).

---

## dbt (gold)

```bash
make dbt-build      # dbt build
make dbt-test
make dbt-docs
make dbt-freshness
```

Модели: `transform/models/staging/`, `transform/models/gold/`.  
Профили: `transform/profiles.yml` (через `DBT_PROFILES_DIR=transform` в Makefile).

---

## Makefile

| Команда | Действие |
|---------|----------|
| `make setup` | `uv sync` + Playwright Chromium |
| `make up` | Инфра + миграции + Dagster |
| `make down` | `compose down` (без profile metabase) |
| `make clean` | `down -v` + удаление `data/` |
| `make logs` / `ps` / `restart` | Логи / статус / рестарт |
| `make ch` | clickhouse-client в контейнере |
| `make dbt-*` | build / test / docs / freshness |
| `make ci` | ruff + pytest + dbt parse |
| `make urls` | Печать URL сервисов |

---

## Разработка

```bash
export PYTHONPATH=src
uv run ruff check src tests
uv run pytest -q
make ci
```

Импорты: пакеты `assets`, `config`, `extractors`, `resources`, `utils`, `checks` живут под `src/` (`PYTHONPATH=src` или Docker `PYTHONPATH=/app/src`).

Локальные данные: `data/downloads`, `data/checkpoints`, `data/cache/osv` (пути из `src/config/paths.py`).

---

## Документация

- [`docs/METABASE_DASHBOARDS.md`](docs/METABASE_DASHBOARDS.md) — дашборды
