"""Ожидание ingest S3Queue → silver (max(_loaded_at) вырос после baseline)."""

import time

from dagster import AssetExecutionContext, MetadataValue, Output

from resources.clickhouse_resource import ClickHouseResource
from utils.clickhouse_sql import scalar as _scalar


def wait_s3queue_loaded(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    table: str,
    before,
    *,
    attempts: int = 12,
    sleep_s: int = 10,
) -> Output[int]:
    """Ждёт count()>0 и max(_loaded_at) > before (или первые строки, если before is None)."""
    fq = f"repo_radar.{table}"
    context.log.info(f"{table}: baseline max(_loaded_at)={before}")

    for attempt in range(attempts):
        after = _scalar(clickhouse, f"SELECT max(_loaded_at) FROM {fq}")
        count = int(_scalar(clickhouse, f"SELECT count() FROM {fq}") or 0)

        if before is None:
            loaded = count > 0 and after is not None
        else:
            loaded = count > 0 and after is not None and after > before

        if loaded:
            context.log.info(f"S3Queue готов: {table} rows={count}, max(_loaded_at)={after}")
            return Output(
                value=count,
                metadata={
                    "row_count": MetadataValue.int(count),
                    "max_loaded_at": MetadataValue.text(str(after)),
                },
            )

        context.log.info(f"Ожидание S3Queue ({table})… {attempt + 1}/{attempts}")
        time.sleep(sleep_s)

    raise RuntimeError(f"S3Queue не обновил {table} за {attempts * sleep_s}s")

