-- Таблица для событий GitHub Archive (silver layer)

CREATE TABLE IF NOT EXISTS repo_radar.silver_github_events
(
    event_id String,
    event_type LowCardinality(String),
    event_time DateTime,
    
    actor_id UInt64,
    actor_login String,
    
    repo_id UInt64,
    repo_name String,
    
    public UInt8,
    
    pr_action Nullable(String),
    pr_number Nullable(UInt32),
    pr_merged Nullable(UInt8),
    
    issue_action Nullable(String),
    issue_number Nullable(UInt32),
    
    push_size Nullable(UInt32),
    push_ref Nullable(String),
    
    release_tag Nullable(String),
    release_name Nullable(String),
    
    _loaded_at DateTime DEFAULT now()
)

ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (repo_name, event_type, event_time, event_id)
SETTINGS index_granularity = 8192;

-- Индекс для быстрого поиска по actor_login
-- (опционально, можно добавить позже при необходимости)



