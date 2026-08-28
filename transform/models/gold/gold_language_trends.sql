{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(event_date, language)',
    settings={'allow_nullable_key': 1}
) }}

with events as (
    select repo_name, event_date, event_type
    from {{ ref('stg_github_events') }}
),

repos as (
    select repo_name, language
    from {{ ref('stg_repos') }}
)

select
    e.event_date                              as event_date,
    coalesce(r.language, 'Unknown')           as language,
    uniqExact(e.repo_name)                     as active_repos,
    countIf(e.event_type = 'WatchEvent')       as watch_events,
    countIf(e.event_type = 'ForkEvent')        as fork_events,
    countIf(e.event_type = 'PushEvent')        as push_events,
    count()                                     as total_events
from events e
left join repos r on e.repo_name = r.repo_name
group by event_date, language

