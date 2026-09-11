CREATE TABLE IF NOT EXISTS repo_radar.bronze_osv_queue
(
    vuln_id String,
    cve_id Nullable(String),
    aliases Nullable(String),
    repo_name Nullable(String),
    ecosystem Nullable(String),
    package Nullable(String),
    summary Nullable(String),
    severity Nullable(String),
    cvss_vector Nullable(String),
    published Nullable(DateTime64(6)),
    modified Nullable(DateTime64(6))
)
ENGINE = S3Queue(
    'http://minio:9000/bronze/osv/dt=*/advisories*.parquet',
    '__MINIO_ROOT_USER__',
    '__MINIO_ROOT_PASSWORD__',
    'Parquet'
)
SETTINGS 
    mode = 'unordered',
    after_processing = 'keep',
    s3queue_loading_retries = 3;

CREATE MATERIALIZED VIEW IF NOT EXISTS repo_radar.mv_osv_to_silver
TO repo_radar.silver_advisories
AS SELECT
    coalesce(vuln_id, '') AS vuln_id,
    cve_id,
    aliases,
    coalesce(repo_name, '') AS repo_name,
    coalesce(ecosystem, 'unknown') AS ecosystem,
    coalesce(package, '') AS package,
    summary,
    coalesce(severity, 'UNKNOWN') AS severity,
    cvss_vector,
    published,
    modified
FROM repo_radar.bronze_osv_queue;


