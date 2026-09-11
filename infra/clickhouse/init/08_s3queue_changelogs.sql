CREATE TABLE IF NOT EXISTS repo_radar.bronze_changelogs_queue
(
    source Nullable(String),
    url Nullable(String),
    version Nullable(String),
    heading Nullable(String),
    release_date Nullable(DateTime64(6)),
    scraped_at Nullable(DateTime64(6))
)
ENGINE = S3Queue(
    'http://minio:9000/bronze/changelogs/dt=*/changelogs*.parquet',
    '__MINIO_ROOT_USER__',
    '__MINIO_ROOT_PASSWORD__',
    'Parquet'
)
SETTINGS
    mode = 'unordered',
    after_processing = 'keep',
    s3queue_loading_retries = 3;

CREATE MATERIALIZED VIEW IF NOT EXISTS repo_radar.mv_changelogs_to_silver
TO repo_radar.silver_changelogs
AS SELECT
    coalesce(source, '') AS source,
    coalesce(url, '') AS url,
    coalesce(version, '') AS version,
    heading,
    release_date,
    coalesce(scraped_at, now()) AS scraped_at
FROM repo_radar.bronze_changelogs_queue;

