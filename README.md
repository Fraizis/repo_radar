# RepoRadar

ELT-платформа для аналитики GitHub-репозиториев: активность (GH Archive), метаданные (GraphQL), уязвимости (OSV), changelogs (Playwright) → MinIO → ClickHouse → dbt → Metabase. Опционально: Slack-алерты на success/failure runs.

---

## Стек


| Слой                | Технология                                |
| ------------------- | ----------------------------------------- |
| Оркестрация         | Dagster                                   |
| Ingestion / клиенты | httpx, dlt (GraphQL source), Playwright   |
| Bronze              | MinIO (Parquet, Hive-пути)                |
| Silver              | ClickHouse (S3Queue + Materialized Views) |
| Gold                | dbt-clickhouse                            |
| UI                  | Metabase                                  |
| Meta DB             | PostgreSQL (Dagster + Metabase)           |
| Python              | 3.12+, пакет-менеджер **uv**              |

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

Seed репозиториев: `config/tracked_repos.yml` (~300 шт.).  
Источники changelog: `config/changelog_sources.yml`.

---

## Требования

- Docker + Docker Compose
- GitHub Personal Access Token (для GraphQL; public metadata достаточно)
- [uv](https://docs.astral.sh/uv/) только для make setup / локалки


---

## Переменные окружения


См. `.env.example`:

| Переменная              | Назначение                                                  |
| ----------------------- | ----------------------------------------------------------- |
| `GITHUB_TOKEN`          | GitHub GraphQL                                              |
| `MINIO_*`               | Bronze S3                                                   |
| `CLICKHOUSE_*`          | Silver/Gold OLAP                                            |
| `POSTGRES_*`            | Dagster + Metabase DB                                       |
| `GHARCHIVE_START_DATE`  | Старт hourly-партиций (`YYYY-MM-DD` или `YYYY-MM-DD-HH:MM`) |
| `SLACK_WEBHOOK_URL`     | Опционально: алерты runs                                    |
| `DAGSTER_WEBSERVER_URL` | Ссылки в Slack                                              |

---

## Быстрый старт

Один раз настроил `.env` → поднял compose → Dagster daemon крутит schedules.
Локально `dagster dev` / `make setup` **не нужны** для работы пайплайна (зависимости уже в образе).

### 1. Клонировать и настроить env

```bash
git clone <repo-url>
cd repo_radar
cp .env.example .env
```

В `.env` минимум:

```bash
GITHUB_TOKEN=ghp_...
GHARCHIVE_START_DATE=2024-01-01
```

Без `GHARCHIVE_START_DATE` партиции стартуют с `now − 3 дня`.


### 2. Поднять всё

```bash
make up
make urls
```

`make up` — MinIO, ClickHouse (+ migrate), Postgres (+ migrate), Dagster webserver/daemon и Metabase.

Опционально для локальной разработки (не для Docker):

```bash
make setup
```

`make setup` — `uv sync` + Playwright Chromium (для changelogs с хоста / локальной разработки).  


### 3. Один раз включить schedules в UI

Открой [http://localhost:3001](http://localhost:3001) → **Automation / Schedules** → **Start** для:


| Schedule                | Когда (UTC)      | Что делает                     |
| ----------------------- | ---------------- | ------------------------------ |
| `gharchive_schedule`    | каждый час в :15 | GH Archive → bronze → silver   |
| `full_pipeline_daily`   | 17:00            | repos + OSV + changelogs + dbt |
| `optimize_silver_daily` | 04:30            | OPTIMIZE silver snapshots      |


Jobs / schedules / checks — `src/jobs.py`, `src/checks/silver_checks.py`.

Без Start jobs сами не пойдут.


### 4. Metabase

[http://localhost:3000](http://localhost:3000) — первый запуск: создать админа, подключить ClickHouse (хост `clickhouse`, порт `8123`). SQL дашбордов — в `docs/METABASE_DASHBOARDS.md`.


### 5. Slack-алерты (опционально)

В `.env`:

```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
DAGSTER_WEBSERVER_URL=http://localhost:3001
```

Без webhook сенсоры no-op. С URL: **FAILURE** любого run; **SUCCESS** по `gharchive_job` / `full_pipeline_job` (и ручным `repos_osv` / `changelogs`); failed asset checks — warning в success-посте.

Сенсоры стартуют сами (`DefaultSensorStatus.RUNNING`) — отдельно Start не нужен, в отличие от schedules.

---

## Порты


| Сервис            | Порт | URL                                            |
| ----------------- | ---- | ---------------------------------------------- |
| Metabase          | 3000 | [http://localhost:3000](http://localhost:3000) |
| Dagster           | 3001 | [http://localhost:3001](http://localhost:3001) |
| MinIO Console     | 9001 | [http://localhost:9001](http://localhost:9001) |
| MinIO API (S3)    | 9002 | [http://localhost:9002](http://localhost:9002) |
| ClickHouse HTTP   | 8123 | [http://localhost:8123](http://localhost:8123) |
| ClickHouse native | 9000 | tcp://localhost:9000                           |
| PostgreSQL        | 5433 | localhost:5433                                 |

MinIO / CH / Postgres дефолтные логины — в `.env.example`.

---

## Структура репозитория

```
repo_radar/
├── config/                    # YAML seed (tracked repos, changelog sources)
├── docs/ 
│   └── METABASE_DASHBOARDS.md # SQL карточек Metabase
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

## CI

GitHub Actions (`.github/workflows/ci.yml`): ruff + pytest + dbt parse на PR и push в `main`. Локально — `make ci`.

---

## Makefile

| Команда                        | Действие                              |
| ------------------------------ | ------------------------------------- |
| `make setup`                   | `uv sync` + Playwright Chromium       |
| `make up`                      | Инфра + миграции + Dagster            |
| `make down`                    | `compose down`                        |
| `make clean`                   | `down -v` + удаление `data/`          |
| `make logs` / `ps` / `restart` | Логи / статус / рестарт               |
| `make ch`                      | clickhouse-client в контейнере        |
| `make dbt-*`                   | build / test / docs / freshness       |
| `make ci`                      | ruff + pytest + dbt parse             |
| `make urls`                    | Печать URL сервисов                   | 

--------------------------

Проект выполнил: Вавилов Д.В. 
@Fraizis

