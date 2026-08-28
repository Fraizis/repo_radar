-- Changelog'и, спарсенные Playwright (bronze → silver).
-- Гранула: одна версия × один источник.
-- ReplacingMergeTree(_loaded_at): повторный прогон обновляет строки без дублей
-- (как silver_repos / silver_advisories).

CREATE TABLE IF NOT EXISTS repo_radar.silver_changelogs
(
    source LowCardinality(String),
    url String,
    version String,

    heading Nullable(String),
    release_date Nullable(DateTime),

    scraped_at DateTime,
    _loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY (source, version)
SETTINGS index_granularity = 8192;


