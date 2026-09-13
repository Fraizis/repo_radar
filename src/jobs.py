"""Jobs и schedules. GH Archive — hourly partitions; снимки — без даты."""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from assets.changelog_assets import bronze_changelogs, silver_changelogs_ready

from assets.partitions import gharchive_partitions
from assets.gharchive_assets import (
    bronze_gharchive, 
    silver_github_events_ready,
)
from assets.github_repos_assets import bronze_github_repos, silver_repos_ready
from assets.optimize_assets import optimize_silver_snapshots
from assets.osv_assets import bronze_osv, silver_advisories_ready


gharchive_job = define_asset_job(
    name="gharchive_job",
    selection=AssetSelection.assets(bronze_gharchive, silver_github_events_ready),
    partitions_def=gharchive_partitions,
)

repos_osv_job = define_asset_job(
    name="repos_osv_job",
    selection=AssetSelection.assets(
        bronze_github_repos,
        silver_repos_ready,
        bronze_osv,
        silver_advisories_ready,
    ),
)

changelogs_job = define_asset_job(
    name="changelogs_job",
    selection=AssetSelection.assets(bronze_changelogs, silver_changelogs_ready),
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=(
        AssetSelection.all()
        - AssetSelection.assets(bronze_gharchive)
        - AssetSelection.assets(silver_github_events_ready)
        - AssetSelection.assets(optimize_silver_snapshots)
    ),
)

optimize_silver_job = define_asset_job(
    name="optimize_silver_job",
    selection=AssetSelection.assets(optimize_silver_snapshots),
)

optimize_silver_schedule = ScheduleDefinition(
    name="optimize_silver_daily",
    job=optimize_silver_job,
    cron_schedule="30 4 * * *",
)

gharchive_schedule = build_schedule_from_partitioned_job(
    job=gharchive_job,
    minute_of_hour=15,
)

repos_osv_schedule = ScheduleDefinition(
    name="repos_osv_every_6h",
    job=repos_osv_job,
    cron_schedule="0 */6 * * *",
)

changelogs_schedule = ScheduleDefinition(
    name="changelogs_daily",
    job=changelogs_job,
    cron_schedule="0 3 * * *",
)

full_pipeline_daily_schedule = ScheduleDefinition(
    name="full_pipeline_daily",
    job=full_pipeline_job,
    cron_schedule="0 17 * * *",
)

all_jobs = [
    gharchive_job,
    repos_osv_job,
    changelogs_job,
    full_pipeline_job,
    optimize_silver_job,
]

all_schedules = [
    gharchive_schedule,
    full_pipeline_daily_schedule,
    optimize_silver_schedule,
]




