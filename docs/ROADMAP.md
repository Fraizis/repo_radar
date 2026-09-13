# RepoRadar — ROADMAP

> **ELT-платформа для аналитики GitHub-репозиториев**  
> Стек: Dagster, dlt, dbt, ClickHouse, MinIO, Metabase

---

## 📋 Обзор

**Цель:** Создать end-to-end data pipeline для анализа активности GitHub-репозиториев с medallion-архитектурой (bronze → silver → gold).

**Объём v1:** ~300 репозиториев, 3 дня GitHub Archive, базовые дашборды.

**Принцип:** Строго последовательное выполнение. Следующий этап начинается только после выполнения критерия готовности (DoD) текущего.

---

## 🏗️ Архитектура
источники → MinIO (bronze parquet) → ClickHouse S3Queue/MV (silver) → dbt (gold) → Metabase
↓                                      ↑
GitHub API ────────────────────────────────┘
↓                                      ↑
OSV API ──────────────────────────────────┘
↓                                      ↑
Playwright ────────────────────────────────┘
↑
Dagster (оркестрация)

---

## 📅 ФАЗА 1: Фундамент (П1-П3)

### П1. Репозиторий и Git

**Задачи:**
- [x] Создать `/home/fraizis/projects/repo-radar`
- [x] Инициализировать git-репозиторий
- [x] Настроить `.gitignore` (`.env`, `.venv`, `__pycache__`, dlt state, данные)
- [x] Создать README-заглушку
- [x] Первый коммит

**DoD:** ✅ Папка является git-репозиторием, есть начальный коммит.

---

### П2. Инфраструктура

**Задачи:**
- [x] `docker-compose.yml` с сервисами:
  - ClickHouse (HTTP 8123, native **9000**)
  - MinIO (API **9002**, консоль 9001) 
  - PostgreSQL **5433** (внутри контейнера 5432; для Dagster + Metabase)
  - Dagster webserver + daemon (3001)
  - Metabase (3000)
- [x] `infra/clickhouse/init/` — скрипты инициализации (БД `repo_radar`, пользователи)
- [x] `Makefile` с командами: `up`, `down`, `logs`, `ch`, `ps`
- [x] `.env.example` с переменными:
  - `GITHUB_TOKEN`
  - MinIO credentials (`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`)
  - ClickHouse пароли
  - PostgreSQL credentials

**DoD:** ✅ `make up` поднимает все сервисы, ClickHouse отвечает на `SELECT 1`, MinIO консоль доступна.

---

### П3. Каркас Python-проекта

**Задачи:**
- [x] `pyproject.toml` + uv с зависимостями:
  - dagster, dagster-webserver, dagster-dlt, dagster-dbt
  - dlt[clickhouse]
  - dbt-clickhouse
  - polars, httpx, playwright
  - clickhouse-connect, pydantic
  - pytest, ruff
- [x] Структура пакета `src/`:
  - `definitions.py` (пустой Dagster Definitions)
  - `resources/`
  - `assets/`
  - `extractors/`
- [x] `config/tracked_repos.yml` — список ~300 репозиториев (Python/Go/Rust)
- [x] `config/changelog_sources.yml` — 3-5 URL для начала
- [x] README с описанием: схема слоёв, как поднять, порты

**DoD:** ✅ `uv sync` успешен, `dagster dev` стартует, `definitions validate` проходит.

---

## 📊 ФАЗА 2: Data Ingestion (П4-П5)

### П4. GitHub Archive → bronze → silver_github_events

**Задачи:**
- [x] Скачать один hourly `.json.gz` файл из GitHub Archive
- [x] Фильтрация событий:
  - Только репозитории из seed
  - Только типы: `WatchEvent`, `ForkEvent`, `ReleaseEvent`, `PullRequestEvent`, `IssuesEvent`, `PushEvent`
- [x] Сохранить Parquet в MinIO: `s3://bronze/gharchive/dt=YYYY-MM-DD/hour=HH/`
- [x] Загрузить в ClickHouse `silver_github_events`:
  - Engine: MergeTree
  - `PARTITION BY toYYYYMMDD(event_time)`
  - `ORDER BY (repo_name, event_type, event_time, event_id)`
- [x] Идемпотентность: повторная загрузка не создаёт дубли (drop partition + insert или ReplacingMergeTree)
- [x] Сохранить один сырой sample без фильтра в `bronze/gharchive_raw_sample/`

**DoD:** ✅ Parquet в MinIO, строки в ClickHouse, повторный запуск не дублирует данные.

---

### П5. GitHub GraphQL → silver_repos

**Задачи:**
- [x] dlt-source для GitHub GraphQL API:
  - Поля: stars, language, forks, pushedAt, description
  - По списку из seed
- [x] Rate limit handling + пагинация
- [x] Сохранить в MinIO (bronze)
- [x] Загрузить в `silver_repos`:
  - Engine: ReplacingMergeTree по `updated_at`
- [x] Использовать `GITHUB_TOKEN` из `.env`

**DoD:** ✅ Таблица `silver_repos` заполнена, повторный прогон обновляет записи без дублей.

---

## 🔄 ФАЗА 3: Трансформации (П6)

### П6. dbt: silver → gold

**Задачи:**
- [x] Создать dbt-проект в `transform/`
- [x] Staging-модели (views) на silver-таблицы
- [x] Gold-модели:
  - `gold_repo_daily` — метрики по дням (stars/forks/pushes/PRs)
  - `gold_language_trends` — популярность языков
  - `gold_rising_repos` — прирост Watch/Fork за 7 дней
- [x] dbt-тесты:
  - `not_null` на ключевые поля
  - `unique` на `event_id`
  - freshness-проверки
- [x] Документация моделей (`dbt docs`)
- [x] Заглушка для `gold_cve_exposure` (реальная реализация в П7)

**DoD:** ✅ `dbt build` проходит без ошибок, витрины отвечают на SELECT-запросы.

---

## 🔒 ФАЗА 4: Дополнительные источники (П7)

### П7. OSV API + Playwright changelogs

**Задачи OSV:**
- [x] Интеграция OSV API для уязвимостей (ecosystem/package из seed)
- [x] Сохранение в bronze (MinIO)
- [x] Загрузка в `silver_advisories`
- [x] dbt-модель `gold_cve_exposure`

**Задачи Playwright:**
- [x] Парсинг changelog по URL из `config/changelog_sources.yml`
- [x] Извлечение: version, date, headings
- [x] Загрузка в `silver_changelogs`
- [x] Соблюдение robots.txt, backoff, checkpoint по URL

**DoD:** ✅ Обе таблицы в ClickHouse заполнены, dbt-модель CVE работает, минимум 3 changelog реально спарсены.

---

## 🎯 ФАЗА 5: Оркестрация (П8)

### П8. Dagster assets + schedules

**Задачи:**
- [x] Создать Dagster assets с зависимостями:
1. `bronze_gharchive` → S3Queue → `silver_github_events` (+ `silver_github_events_ready`)
2. `bronze_github_repos` → S3Queue → `silver_repos` (+ `silver_repos_ready`)
3. `bronze_osv` → S3Queue → `silver_advisories` (+ `silver_advisories_ready`)
4. `bronze_changelogs` → S3Queue → `silver_changelogs` (+ `silver_changelogs_ready`)
5. `dbt_build` (lineage от `*_ready`)
- [x] Asset checks:
  - Row count > 0
  - Freshness (данные не старше X часов)
  - Аномалии объёма (vs медиана)
- [x] Schedules:
- GitHub Archive: каждый час (:15)
- full_pipeline (repos/OSV/changelogs/dbt): раз в день 17:00
- optimize silver snapshots: 04:30

**DoD:** ✅ В Dagster UI виден lineage граф, один job запускает всю цепочку end-to-end.

---

## 🚀 ФАЗА 6: Визуализация и финал (П9)

### П9. Metabase, демо и документация

**Задачи Metabase:**
- [x] Подключить к ClickHouse
- [x] Создать дашборды:
  - **Repo Pulse** — события по дням и типам
  - **Rising Repos** — топ за 7 дней
  - **Language Trends** — популярность языков
  - **CVE Exposure** — репозитории с уязвимостями

- [x] Аллерты в Slack
- [x] CI/CD: `ruff` + `dbt parse` + pytest

**Задачи автоматизации:**
- [ ] Вывести порты: Metabase :3000, Dagster :3001, MinIO console :9001, MinIO API :9002, ClickHouse :8123, Postgres :5433

**Задачи документации:**
- [ ] Обновить README:
  - Архитектурная схема
  - Инструкции по запуску
  - Скриншоты дашбордов 
  - Блок "Что было бы в продакшене"
- [ ] Формулировка для резюме:
  > "RepoRadar — ELT-платформа для аналитики GitHub-репозиториев (Dagster, dlt, dbt, ClickHouse): GitHub Archive + API + scraping → MinIO → medallion-архитектура, инкрементальная загрузка, тесты качества, дашборды Metabase"

**DoD:** ✅ `make up && make demo` с нуля даёт работающие дашборды с данными за 3 дня.

---

## 📦 Итоговая структура проекта

repo-radar/
├── README.md
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── .env.example
├── .gitignore
├── config/
│   ├── tracked_repos.yml
│   └── changelog_sources.yml
├── infra/
│   └── clickhouse/
│       └── init/
├── src/
│   ├── definitions.py
│   ├── resources/
│   ├── assets/
│   └── extractors/
├── transform/              # dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
└── tests/

---

## 🚫 Вне scope v1

Следующие технологии **НЕ используются** в v1:
- ❌ Kafka
- ❌ Spark
- ❌ Iceberg
- ❌ Airflow
- ❌ Kubernetes
- ❌ HeadHunter API

**Возможные улучшения после v1:**
- CI/CD: `ruff` + `dbt parse` + pytest

---

## 📊 Технологический стек

| Компонент | Технология | Назначение |
|-----------|-----------|------------|
| Оркестрация | Dagster | Управление пайплайном, lineage, мониторинг |
| Ingestion | dlt | Извлечение данных из источников |
| Storage (bronze) | MinIO | Объектное хранилище для сырых данных |
| Storage (silver/gold) | ClickHouse | Колоночная СУБД для аналитики |
| Трансформации | dbt | Модели данных, тесты, документация |
| Визуализация | Metabase | Дашборды и отчёты |
| Scraping | Playwright | Парсинг changelog |
| Язык | Python 3.12 | Основной язык разработки |
| Пакетный менеджер | uv | Быстрое управление зависимостями |
| Контейнеризация | Docker Compose | Локальная инфраструктура |

---

## ✅ Чек-лист прогресса

- [x] **П1** — Репозиторий создан
- [x] **П2** — Инфраструктура поднимается
- [x] **П3** — Python-каркас готов
- [x] **П4** — GitHub Archive → ClickHouse
- [x] **П5** — GitHub GraphQL → ClickHouse
- [x] **П6** — dbt-модели работают
- [x] **П7** — OSV + Playwright интегрированы
- [x] **П8** — Dagster оркестрирует весь пайплайн
- [ ] **П9** — Демо готово, проект в резюме

---



