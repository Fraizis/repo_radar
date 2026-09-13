"""Assets: Playwright changelogs → bronze → silver_changelogs (S3Queue)."""

from dagster import AssetExecutionContext, asset

from assets._common import (
    attach_baseline,
    bronze_output,
    read_silver_baseline,
    silver_ready_or_skip,
)
from config.paths import CHANGELOG_SOURCES_YAML, CHECKPOINT_DIR
from extractors.changelog.extractor_changelog import ChangelogExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource


@asset(
    group_name="changelogs",
    description="Playwright changelog scraping → bronze (Parquet в MinIO)",
)
def bronze_changelogs(
    context: AssetExecutionContext,
    minio: MinIOResource,
    clickhouse: ClickHouseResource,
):
    before = read_silver_baseline(context, clickhouse, "silver_changelogs")

    extractor = ChangelogExtractor(
        sources_path=CHANGELOG_SOURCES_YAML,
        checkpoint_dir=CHECKPOINT_DIR,
        object_store=minio.get_store(),
    )
    result = extractor.process_snapshot()
    attach_baseline(result, before)

    parquet_uri = result["parquet_uri"]
    if not parquet_uri:
        context.log.warning(
            "Changelogs: новых данных нет — bronze пропущен, ready сделает skip"
        )
    else:
        context.log.info(
            f"✓ Changelogs: {result['rows_count']} строк "
            f"из {result['parsed_sources']} источников"
        )

    meta = {
        "rows_count": result["rows_count"],
        "parsed_sources": result["parsed_sources"],
        "has_data": bool(parquet_uri),
    }
    if parquet_uri:
        meta["parquet_uri"] = parquet_uri

    return bronze_output(result, meta)


@asset(group_name="changelogs")
def silver_changelogs_ready(
    context: AssetExecutionContext,
    bronze_changelogs: dict,
    clickhouse: ClickHouseResource,
):
    """Барьер после bronze; skip, если parquet не писали."""
    return silver_ready_or_skip(
        context, clickhouse, "silver_changelogs", bronze_changelogs
    )


