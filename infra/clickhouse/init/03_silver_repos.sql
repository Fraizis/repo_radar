-- Снимок метаданных репозиториев (GitHub GraphQL → silver)
-- ReplacingMergeTree(updated_at): при слиянии побеждает строка
-- с большим updated_at. Ключ уникальности — ORDER BY (repo_name).
-- SELECT ... FINAL или OPTIMIZE TABLE ... FINAL, если нужна дедуп сразу.

CREATE TABLE IF NOT EXISTS repo_radar.silver_repos
(
    repo_id UInt64,
    repo_name String,
    owner String,
    name String,

    description Nullable(String),
    language LowCardinality(Nullable(String)),

    stars UInt64,
    forks UInt64,
    
    pushed_at Nullable(DateTime),
    updated_at DateTime,
    created_at Nullable(DateTime),

    ecosystem LowCardinality(String),
    package String,

    _loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (repo_name)
SETTINGS index_granularity = 8192;


