"""Общие хелперы bronze → silver_ready для Dagster assets.

Паттерн:
  1. ``read_silver_baseline`` — max(_loaded_at) до записи
  2. extractor.process_* → result dict
  3. ``attach_baseline`` + ``bronze_output``
  4. ``silver_ready_or_skip`` — wait S3Queue (или skip без parquet)
"""

from __future__ import annotations

from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output

from resources.clickhouse_resource import ClickHouseResource
from utils.clickhouse_sql import scalar as _scalar
from utils.s3queue_wait import wait_s3queue_loaded

MetadataInput = dict[str, Any]


def read_silver_baseline(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    table: str,
) -> Any:
    """``SELECT max(_loaded_at) FROM repo_radar.{table}`` + лог.

    Args:
        table: Имя silver-таблицы без схемы, например ``silver_repos``.
    """
    before = _scalar(
        clickhouse,
        f"SELECT max(_loaded_at) FROM repo_radar.{table}",
    )
    context.log.info(f"max(_loaded_at) до bronze: {before}")
    return before


def attach_baseline(result: dict, before: Any) -> dict:
    """Кладёт baseline в result для передачи в ``*_ready`` asset."""
    result["silver_max_loaded_at_before"] = before
    return result


def _wrap_metadata(raw: MetadataInput) -> dict[str, MetadataValue]:
    """int/bool/str/None → MetadataValue; уже обёрнутое пропускает."""
    out: dict[str, MetadataValue] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if isinstance(value, MetadataValue):
            out[key] = value
        elif isinstance(value, bool):
            out[key] = MetadataValue.bool(value)
        elif isinstance(value, int) and not isinstance(value, bool):
            out[key] = MetadataValue.int(value)
        else:
            out[key] = MetadataValue.text(str(value))
    return out


def bronze_output(result: dict, metadata: MetadataInput) -> Output[dict]:
    """``Output`` bronze-ассета с baseline в metadata (если есть в result)."""
    meta = dict(metadata)
    if "silver_max_loaded_at_before" not in meta and "silver_max_loaded_at_before" in result:
        meta["silver_max_loaded_at_before"] = result["silver_max_loaded_at_before"]
    return Output(value=result, metadata=_wrap_metadata(meta))


def silver_ready_or_skip(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    table: str,
    bronze_result: dict,
    *,
    skip_if_no_parquet: bool = True,
) -> Output[int]:
    """Барьер S3Queue после bronze.

    Args:
        table: silver-таблица.
        bronze_result: dict от upstream bronze (с ``parquet_uri``, baseline).
        skip_if_no_parquet: Если True и нет ``parquet_uri`` → ``Output(0)``
            без wait (changelogs / пустой час). Для OSV/GraphQL можно
            оставить True — туда не дойдут без parquet (bronze уже raise).
    """
    if skip_if_no_parquet and not bronze_result.get("parquet_uri"):
        context.log.warning("Нет нового parquet — S3Queue wait пропущен")
        return Output(
            value=0,
            metadata={"skipped": MetadataValue.bool(True)},
        )

    return wait_s3queue_loaded(
        context,
        clickhouse,
        table,
        bronze_result.get("silver_max_loaded_at_before"),
    )

