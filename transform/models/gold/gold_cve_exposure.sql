{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(severity, repo_name)',
    settings={'allow_nullable_key': 1}
) }}

with advisories as (
    select * from {{ ref('stg_advisories') }}
),

repos as (
    select
        repo_name,
        language,
        stars
    from {{ ref('stg_repos') }}
)

select
    a.repo_name,
    a.ecosystem,
    a.package,
    a.vuln_id,
    a.cve_id,
    a.severity,
    a.summary,
    a.published,
    r.language,
    r.stars                as repo_stars
from advisories a
left join repos r on a.repo_name = r.repo_name

