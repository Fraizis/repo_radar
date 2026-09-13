"""Assets for OSV: seed → bronze_osv → silver_advisories (через S3Queue)."""

from dagster import AssetExecutionContext, asset

from assets._common import (
    attach_baseline,
    bronze_output,
    read_silver_baseline,
    silver_ready_or_skip,
)
from config.paths import TRACKED_REPOS_YAML
from extractors.osv.extractor_osv import OSVExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource


@asset(
    group_name="osv",
    description="OSV.dev API → bronze (Parquet в MinIO)",
)
def bronze_osv(
    context: AssetExecutionContext,
    minio: MinIOResource,
    clickhouse: ClickHouseResource,
):
    before = read_silver_baseline(context, clickhouse, "silver_advisories")

    extractor = OSVExtractor(
        tracked_repos_path=TRACKED_REPOS_YAML,
        object_store=minio.get_store(),
    )
    result = extractor.process_snapshot()
    if not result["parquet_uri"]:
        raise RuntimeError("OSV: пустой снимок — parquet не записан")

    attach_baseline(result, before)
    return bronze_output(
        result,
        {
            "advisories_count": result["advisories_count"],
            "parquet_uri": result["parquet_uri"],
        },
    )


@asset(group_name="osv")
def silver_advisories_ready(
    context: AssetExecutionContext,
    bronze_osv: dict,
    clickhouse: ClickHouseResource,
):
    return silver_ready_or_skip(
        context, clickhouse, "silver_advisories", bronze_osv
    )


