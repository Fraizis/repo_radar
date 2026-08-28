with source as (
    select *
    from {{ source('silver', 'silver_github_events') }}
    final
)

select
    event_id,
    event_type,
    event_time,
    toDate(event_time)            as event_date,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    public,
    pr_action,
    pr_number,
    pr_merged,
    issue_action,
    issue_number,
    push_size,
    push_ref,
    release_tag,
    release_name
from source


