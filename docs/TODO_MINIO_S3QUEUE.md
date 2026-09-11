# MinIO-only bronze + ClickHouse S3Queue — остаток работ

Статус на момент выноса: шаги 1–8 по сути закрыты.
Цель: bronze только в MinIO → S3Queue/MV → silver → dbt; Python не грузит silver.

---

## ✅ Сделано (не трогать без нужды)

- [x] 1. MinioStore: put_bytes / get_bytes / put_dataframe / exists
- [x] 2–4. Bronze writers + extractors → MinIO, `parquet_uri`
- [x] 5. Bronze Dagster assets, без локального bronze_path
- [x] 6. ClickHouse config: listen.xml, s3.xml, keeper.xml
- [x] 7. S3Queue + MV (06–09), migrate через Makefile `run --rm`
- [x] 8. Убраны Python silver assets; checks на bronze; jobs на bronze
- [x] 8+. `silver_advisories_ready` (wait + max(_loaded_at)) для OSV
- [x] Общий `utils/clickhouse_sql.scalar`

---

## 🔲 Осталось — приоритет

### A. Шаг 9 — OPTIMIZE FINAL (ReplacingMergeTree)

Зачем: silver_repos / silver_advisories / silver_changelogs без периодического merge держат дубли.

- [x] Создать `src/assets/optimize_assets.py`
  - asset `optimize_silver_snapshots`
  - `OPTIMIZE TABLE repo_radar.{silver_repos,silver_advisories,silver_changelogs} FINAL`
- [x] Добавить asset в `src/definitions.py`
- [x] В `src/jobs.py`:
  - `optimize_silver_job`
  - schedule (например `30 4 * * *`)
  - решить: включать ли в `full_pipeline_job` или исключить
- [x] Один раз materialize вручную и проверить в CH

### B. E2E-проверка пайплайна

- [x] Materialize `bronze_osv` → `silver_advisories_ready`
- [x] `SELECT count(), max(_loaded_at) FROM repo_radar.silver_advisories`
- [x] Materialize `bronze_github_repos` → проверить `silver_repos`
- [x] Materialize партицию `bronze_gharchive` → `silver_github_events`
- [x] При необходимости `bronze_changelogs` → `silver_changelogs`
- [x] `dbt_build` после появления свежих silver
- [x] Убедиться, что asset checks проходят (или понятны WARN)

### D. ClickHouseLoader (опционально)

- [ ] Поддержка `s3://…` через MinioStore.get_bytes + polars
- [ ] Или пометить loader deprecated / оставить только для offline debug

### E. Wait для остальных источников (опционально)

Сейчас wait только у OSV.

- [x] `silver_repos_ready` (deps=[bronze_github_repos])
- [x] `silver_changelogs_ready` (deps=[bronze_changelogs]; учесть skip без нового parquet)
- [x] `silver_github_events_ready` (partitioned, deps=[bronze_gharchive])
- [x] Добавить в jobs / definitions

Тот же паттерн: `max(_loaded_at)` before/after.

### F. Dagster lineage dbt (опционально)

Сейчас `dbt_assets.py` мапит source → `AssetKey(["silver_github_events"])` и т.п., python silver нет.

- [x] Обновить `SilverSourceTranslator`: source → соответствующий `bronze_*` / `*_ready`
- [x] Обновить docstring в `dbt_assets.py`

### G. Schedules / docs (мелочи)

- [x] Включить `repos_osv_schedule` / `changelogs_schedule` в `all_schedules`, если нужны
- [x] `docs/ROADMAP.md`: убрать S3Queue из «после v1» / отметить сделанным
- [x] README: схема bronze = MinIO → S3Queue → silver (без локального диска)
- [x] Docstring'и assets («→ silver») привести к факту (S3Queue)

### H. Жёсткость / prod (по желанию)

- [ ] `users.d` с `allow_experimental_s3queue` (не только флаг в migrate)
- [ ] Named collection / не хардкодить креды MinIO в SQL через sed
- [ ] Retry в migrate до native-порта (уже частично решено listen.xml)
- [ ] Убрать `container_name` у one-shot migrate (мелочь)

---

## Рекомендуемый порядок работы

1. **A** — OPTIMIZE (закрыть шаг 9 плана)
2. **B** — E2E OSV + один gharchive час
3. **C** — починить scripts / `make demo`
4. **E** — wait для repos/changelogs по необходимости
5. **F–G** — lineage и доки
6. **D/H** — когда понадобится backfill/prod

---

## Критерий «готово целиком»

- [ ] `make up` зелёный, 4 S3Queue + 4 MV
- [ ] Materialize bronze_* → строки в silver_* без Python INSERT
- [ ] `make demo` / run-скрипты не падают на `bronze_dir`
- [ ] OPTIMIZE по schedule или в maintenance job
- [ ] dbt build после silver проходит

