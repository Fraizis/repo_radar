{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='growth_score',
    settings={'allow_nullable_key': 1}
) }}

with bounds as (
    select max(event_date) as max_date
    from {{ ref('stg_github_events') }}
),

windowed as (
    select
        e.repo_name,
        countIf(e.event_type = 'WatchEvent') as new_stars_7d,
        countIf(e.event_type = 'ForkEvent')  as new_forks_7d
    from {{ ref('stg_github_events') }} e
    cross join bounds b
    where e.event_date > b.max_date - 7
    group by e.repo_name
)

select
    w.repo_name,
    w.new_stars_7d,
    w.new_forks_7d,
    (w.new_stars_7d + w.new_forks_7d) as growth_score,
    r.language,
    r.stars                            as total_stars
from windowed w
left join {{ ref('stg_repos') }} r on w.repo_name = r.repo_name
order by growth_score desc


