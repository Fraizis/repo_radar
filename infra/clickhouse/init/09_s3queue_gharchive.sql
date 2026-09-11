CREATE TABLE IF NOT EXISTS repo_radar.bronze_gharchive_queue
(
    event_id Nullable(String),
    event_type Nullable(String),
    event_time Nullable(String),
    actor_id Nullable(Int64),
    actor_login Nullable(String),
    repo_id Nullable(Int64),
    repo_name Nullable(String),
    public Nullable(Bool),
    pr_action Nullable(String),
    pr_number Nullable(Int32),
    pr_merged Nullable(Bool),
    issue_action Nullable(String),
    issue_number Nullable(Int32),
    push_size Nullable(Int32),
    push_ref Nullable(String),
    release_tag Nullable(String),
    release_name Nullable(String)
)
ENGINE = S3Queue(
    'http://minio:9000/bronze/gharchive/dt=*/hour=*/events.parquet',
    '__MINIO_ROOT_USER__',
    '__MINIO_ROOT_PASSWORD__',
    'Parquet'
)
SETTINGS
    mode = 'unordered',
    after_processing = 'keep',
    s3queue_loading_retries = 3;

CREATE MATERIALIZED VIEW IF NOT EXISTS repo_radar.mv_gharchive_to_silver
TO repo_radar.silver_github_events
AS SELECT
    coalesce(q.event_id, '') AS event_id,
    coalesce(q.event_type, '') AS event_type,
    parseDateTimeBestEffortOrNull(
        replaceRegexpOne(coalesce(q.event_time, ''), '[Zz]$', '')
    ) AS event_time,
    coalesce(q.actor_id, 0) AS actor_id,
    coalesce(q.actor_login, '') AS actor_login,
    coalesce(q.repo_id, 0) AS repo_id,
    coalesce(q.repo_name, '') AS repo_name,
    toUInt8(ifNull(q.public, true)) AS public,
    q.pr_action,
    q.pr_number,
    if(isNull(q.pr_merged), NULL, toUInt8(q.pr_merged)) AS pr_merged,
    q.issue_action,
    q.issue_number,
    q.push_size,
    q.push_ref,
    q.release_tag,
    q.release_name
FROM repo_radar.bronze_gharchive_queue AS q
WHERE parseDateTimeBestEffortOrNull(
    replaceRegexpOne(coalesce(q.event_time, ''), '[Zz]$', '')
) IS NOT NULL;




