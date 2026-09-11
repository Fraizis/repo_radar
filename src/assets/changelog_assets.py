"""Assets: Playwright changelogs → bronze → silver_changelogs (S3Queue)."""

from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from extractors.changelog.extractor_changelog import ChangelogExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource
from utils.clickhouse_sql import scalar as _scalar
from utils.s3queue_wait import wait_s3queue_loaded

project_root = Path(__file__).parent.parent.parent


@asset(
    group_name="changelogs",
    description="Playwright changelog scraping → bronze (Parquet в MinIO)",
)
def bronze_changelogs(
    context: AssetExecutionContext,
    minio: MinIOResource,
    clickhouse: ClickHouseResource,
) -> Output[dict]:
    before = _scalar(
        clickhouse,
        "SELECT max(_loaded_at) FROM repo_radar.silver_changelogs",
    )
    context.log.info(f"max(_loaded_at) до bronze: {before}")

    extractor = ChangelogExtractor(
        sources_path=project_root / "config" / "changelog_sources.yml",
        checkpoint_dir=Path("./data/checkpoints"),
        object_store=minio.get_store(),
    )
    result = extractor.process_snapshot()
    result["silver_max_loaded_at_before"] = before

    parquet_uri = result["parquet_uri"]
    if not parquet_uri:
        context.log.warning(
            "Changelogs: новых данных нет — bronze пропущен, ready сделает skip"
        )

    metadata: dict = {
        "rows_count": MetadataValue.int(result["rows_count"]),
        "parsed_sources": MetadataValue.int(result["parsed_sources"]),
        "has_data": MetadataValue.bool(bool(parquet_uri)),
        "silver_max_loaded_at_before": MetadataValue.text(str(before)),
    }
    if parquet_uri:
        metadata["parquet_uri"] = MetadataValue.text(parquet_uri)
        context.log.info(
            f"✓ Changelogs: {result['rows_count']} строк из {result['parsed_sources']} источников"
        )

    return Output(value=result, metadata=metadata)


@asset(group_name="changelogs")
def silver_changelogs_ready(
    context: AssetExecutionContext,
    bronze_changelogs: dict,
    clickhouse: ClickHouseResource,
) -> Output[int]:
    """Барьер после bronze; skip, если parquet не писали."""
    if not bronze_changelogs.get("parquet_uri"):
        context.log.warning("Нет нового parquet — S3Queue wait пропущен")
        return Output(
            value=0,
            metadata={"skipped": MetadataValue.bool(True)},
        )

    return wait_s3queue_loaded(
        context,
        clickhouse,
        "silver_changelogs",
        bronze_changelogs.get("silver_max_loaded_at_before"),
    )


