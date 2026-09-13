import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from assets.changelog_assets import bronze_changelogs, silver_changelogs_ready
from assets.dbt_assets import dbt_build, dbt_project
from assets.gharchive_assets import bronze_gharchive, silver_github_events_ready
from assets.github_repos_assets import bronze_github_repos, silver_repos_ready
from assets.optimize_assets import optimize_silver_snapshots
from assets.osv_assets import bronze_osv, silver_advisories_ready
from checks.silver_checks import all_silver_checks
from jobs import all_jobs, all_schedules
from resources.clickhouse_resource import ClickHouseResource
from resources.github_resource import GitHubResource
from resources.minio_resource import MinIOResource
from resources.slack_resource import SlackResource
from sensors.slack_sensors import (
    slack_on_job_success,
    slack_on_run_failure,
)

"""Корневой объект Dagster: список assets и именованные resources."""
defs = Definitions(
    assets=[
        bronze_gharchive,
        silver_github_events_ready,
        bronze_github_repos,
        silver_repos_ready,
        bronze_osv,
        silver_advisories_ready,
        bronze_changelogs,
        silver_changelogs_ready,
        dbt_build,
        optimize_silver_snapshots,
    ],
    asset_checks=all_silver_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=[
        slack_on_job_success,
        slack_on_run_failure,
    ],
    resources={
        "clickhouse": ClickHouseResource(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD",""),
            database="repo_radar",
        ),
        "minio": MinIOResource(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9002"),
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            bucket=os.getenv("MINIO_BUCKET", "bronze"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            ),
        "github": GitHubResource(
            token=os.getenv("GITHUB_TOKEN", ""),
        ),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=str(dbt_project.project_dir),
        ),
        "slack": SlackResource(
            webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
            dagster_base_url=os.getenv("DAGSTER_WEBSERVER_URL", "http://localhost:3001"),
        ),
    }
)



