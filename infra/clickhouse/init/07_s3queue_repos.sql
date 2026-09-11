CREATE TABLE IF NOT EXISTS repo_radar.bronze_repos_queue
(
    repo_id Nullable(Int64),
    repo_name Nullable(String),
    owner Nullable(String),
    name Nullable(String),
    description Nullable(String),
    language Nullable(String),
    stars Nullable(Int64),
    forks Nullable(Int64),
    pushed_at Nullable(DateTime64(6)),
    updated_at Nullable(DateTime64(6)),
    created_at Nullable(DateTime64(6)),
    ecosystem Nullable(String),
    package Nullable(String)
)
ENGINE = S3Queue(
    'http://minio:9000/bronze/github_repos/dt=*/repos*.parquet',
    '__MINIO_ROOT_USER__',
    '__MINIO_ROOT_PASSWORD__',
    'Parquet'
)
SETTINGS
    mode = 'unordered',
    after_processing = 'keep',
    s3queue_loading_retries = 3;

CREATE MATERIALIZED VIEW IF NOT EXISTS repo_radar.mv_repos_to_silver
TO repo_radar.silver_repos
AS SELECT
    coalesce(repo_id, 0) AS repo_id,
    coalesce(repo_name, '') AS repo_name,
    coalesce(owner, '') AS owner,
    coalesce(name, '') AS name,
    description,
    language,
    coalesce(stars, 0) AS stars,
    coalesce(forks, 0) AS forks,
    pushed_at,
    coalesce(updated_at, now()) AS updated_at,
    created_at,
    coalesce(ecosystem, 'unknown') AS ecosystem,
    coalesce(package, '') AS package
FROM repo_radar.bronze_repos_queue;


