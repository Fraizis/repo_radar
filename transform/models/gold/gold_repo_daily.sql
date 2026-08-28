{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(event_date, repo_name)',
    partition_by='toYYYYMM(event_date)', 
    settings={'allow_nullable_key': 1}
) }}

select
    event_date,
    repo_name,
    countIf(event_type = 'WatchEvent')        as watch_events,
    countIf(event_type = 'ForkEvent')         as fork_events,
    countIf(event_type = 'PushEvent')         as push_events,
    countIf(event_type = 'PullRequestEvent')  as pr_events,
    countIf(event_type = 'IssuesEvent')       as issue_events,
    countIf(event_type = 'ReleaseEvent')      as release_events,
    count()                                   as total_events,
    uniqExact(actor_login)                    as unique_actors
from {{ ref('stg_github_events') }}
group by event_date, repo_name


