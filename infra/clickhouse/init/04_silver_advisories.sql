-- Уязвимости из OSV.dev (bronze → silver).
-- Гранула: одна уязвимость × один затронутый репозиторий (repo_name).
-- ReplacingMergeTree(_loaded_at): повторный прогон обновляет строки,
-- не плодя дубли (как в silver_github_events).

CREATE TABLE IF NOT EXISTS repo_radar.silver_advisories
(
    vuln_id String,
    cve_id Nullable(String),
    aliases Nullable(String),

    repo_name String,
    ecosystem LowCardinality(String),
    package String,

    summary Nullable(String),
    severity LowCardinality(String),
    cvss_vector Nullable(String),

    published Nullable(DateTime),
    modified Nullable(DateTime),

    _loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY (repo_name, package, vuln_id)
SETTINGS index_granularity = 8192;

