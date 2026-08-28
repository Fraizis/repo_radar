with source as (
    select *
    from {{ source('silver', 'silver_repos') }}
    final
)

select
    repo_id,
    repo_name,
    owner,
    name,
    description,
    language,
    stars,
    forks,
    pushed_at,
    updated_at,
    created_at,
    ecosystem,
    package
from source

