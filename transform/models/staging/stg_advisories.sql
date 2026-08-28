with source as (
    select *
    from {{ source('silver', 'silver_advisories') }}
    final
)

select
    vuln_id,
    cve_id,
    aliases,
    repo_name,
    ecosystem,
    package,
    summary,
    severity,
    cvss_vector,
    published,
    modified
from source

