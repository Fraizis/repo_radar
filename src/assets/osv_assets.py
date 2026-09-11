"""Assets for OSV: seed → bronze_osv → silver_advisories (через S3Queue)."""

from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from extractors.osv.extractor_osv import OSVExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource
from utils.clickhouse_sql import scalar as _scalar
from utils.s3queue_wait import wait_s3queue_loaded

project_root = Path(__file__).parent.parent.parent


@asset(
    group_name="osv",
    description="OSV.dev API → bronze (Parquet в MinIO)",
)
def bronze_osv(
    context: AssetExecutionContext,
    minio: MinIOResource,
    clickhouse: ClickHouseResource,
) -> Output[dict]:
    before = _scalar(
        clickhouse,
        "SELECT max(_loaded_at) FROM repo_radar.silver_advisories",
    )
    context.log.info(f"max(_loaded_at) до bronze: {before}")

    store = minio.get_store()
    extractor = OSVExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        object_store=store,
    )
    result = extractor.process_snapshot()
    if not result["parquet_uri"]:
        raise RuntimeError("OSV: пустой снимок — parquet не записан")

    result["silver_max_loaded_at_before"] = before

    return Output(
        value=result,
        metadata={
            "advisories_count": MetadataValue.int(result["advisories_count"]),
            "parquet_uri": MetadataValue.text(result["parquet_uri"]),
            "silver_max_loaded_at_before": MetadataValue.text(str(before)),
        },
    )


@asset(group_name="osv")
def silver_advisories_ready(
    context: AssetExecutionContext,
    bronze_osv: dict,
    clickhouse: ClickHouseResource,
) -> Output[int]:
    """Барьер: S3Queue долил silver после ЭТОГО bronze (для dbt / downstream)."""
    return wait_s3queue_loaded(
        context,
        clickhouse,
        "silver_advisories",
        bronze_osv.get("silver_max_loaded_at_before"),
    )

