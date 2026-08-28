with src as (
    select *
    from {{ source('silver', 'silver_changelogs') }}
    final
)

select
    source,
    url,
    version,
    heading,
    release_date,
    scraped_at
from src

