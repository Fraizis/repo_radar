"""Assets: GitHub GraphQL → bronze → silver_repos (S3Queue)."""

from dagster import AssetExecutionContext, asset

from assets._common import (
    attach_baseline,
    bronze_output,
    read_silver_baseline,
    silver_ready_or_skip,
)
from config.paths import TRACKED_REPOS_YAML
from extractors.github.extractor_repos import GitHubReposExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.github_resource import GitHubResource
from resources.minio_resource import MinIOResource


@asset(
    group_name="github_repos",
    description="GitHub GraphQL→ bronze Parquet в MinIO (S3Queue → silver)",
)
def bronze_github_repos(
    context: AssetExecutionContext,
    minio: MinIOResource,
    github: GitHubResource,
    clickhouse: ClickHouseResource,
):
    before = read_silver_baseline(context, clickhouse, "silver_repos")

    extractor = GitHubReposExtractor(
        tracked_repos_path=TRACKED_REPOS_YAML,
        github_token=github.require_token(),
        object_store=minio.get_store(),
    )
    result = extractor.process_snapshot()
    if not result["parquet_uri"]:
        raise RuntimeError("Пустой снимок GraphQL — parquet не записан")

    attach_baseline(result, before)
    context.log.info(f"✓ Снимок: {result['repos_count']} репо")

    return bronze_output(
        result,
        {
            "repos_count": result["repos_count"],
            "seed_count": result["seed_count"],
            "parquet_uri": result["parquet_uri"],
        },
    )


@asset(group_name="github_repos")
def silver_repos_ready(
    context: AssetExecutionContext,
    bronze_github_repos: dict,
    clickhouse: ClickHouseResource,
):
    """Барьер: S3Queue долил silver_repos после этого bronze."""
    return silver_ready_or_skip(
        context, clickhouse, "silver_repos", bronze_github_repos
    )


