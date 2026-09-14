"""GitHub Archive: hourly partition → MinIO bronze → S3Queue → silver_github_events."""

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from assets._common import attach_baseline, bronze_output, read_silver_baseline
from assets.partitions import gharchive_partitions, partition_dt
from config.paths import DOWNLOAD_DIR, TRACKED_REPOS_YAML
from extractors.gharchive.clickhouse_load import ensure_gharchive_hour
from extractors.gharchive.extractor_gh import GitHubArchiveExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource


@asset(
    group_name="github_archive",
    partitions_def=gharchive_partitions,
    description="GitHub Archive за 1 час → MinIO parquet (S3Queue → silver)",
)
def bronze_gharchive(
    context: AssetExecutionContext,
    minio: MinIOResource,
    clickhouse: ClickHouseResource,
):
    target_dt = partition_dt(context)
    context.log.info(f"GH Archive {target_dt:%Y-%m-%d %H:00} UTC")

    before = read_silver_baseline(context, clickhouse, "silver_github_events")

    extractor = GitHubArchiveExtractor(
        tracked_repos_path=TRACKED_REPOS_YAML,
        download_dir=DOWNLOAD_DIR,
        object_store=minio.get_store(),
    )
    result = extractor.process_hour(dt=target_dt, save_raw_sample=True)
    attach_baseline(result, before)

    return bronze_output(
        result,
        {
            "datetime": target_dt.isoformat(),
            "events_count": result["events_count"],
            "parquet_uri": result["parquet_uri"] or "",
            "tracked_repos": len(extractor.tracked_repos),
        },
    )


@asset(
    group_name="github_archive",
    partitions_def=gharchive_partitions,
)
def silver_github_events_ready(
    context: AssetExecutionContext,
    bronze_gharchive: dict,
    clickhouse: ClickHouseResource,
):
    """Барьер после часа; skip, если events=0 / нет parquet."""
    if not bronze_gharchive.get("parquet_uri"):
        context.log.warning("Пустой час (нет parquet) — S3Queue wait пропущен")
        return Output(
            value=0,
            metadata={"skipped": MetadataValue.bool(True)},
        )
    return ensure_gharchive_hour(
        context,
        clickhouse,
        partition_dt(context),
    )


