"""GitHub Archive: hourly partition → MinIO bronze → S3Queue → silver_github_events."""

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    HourlyPartitionsDefinition,
    MetadataValue,
    Output,
    asset,
)

sys.path.insert(0, str(Path(__file__).parent.parent))

from extractors.gharchive.extractor_gh import GitHubArchiveExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource
from utils.clickhouse_sql import scalar as _scalar
from utils.s3queue_wait import ensure_gharchive_hour


def _gharchive_start_date() -> str:
    raw = os.getenv("GHARCHIVE_START_DATE", "").strip()
    if raw:
        if len(raw) == 10:
            return f"{raw}-00:00"
        return raw
    start = datetime.now(UTC).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(days=3)
    return start.strftime("%Y-%m-%d-%H:%M")


gharchive_partitions = HourlyPartitionsDefinition(
    start_date=_gharchive_start_date(),
    timezone="UTC",
    end_offset=0,
)


def _partition_dt(context: AssetExecutionContext) -> datetime:
    """UTC-naive datetime часа партиции — так ждёт downloader (hour в имени файла)."""
    key = context.partition_key
    return datetime.strptime(key, "%Y-%m-%d-%H:%M")

@asset(
    group_name="github_archive",
    partitions_def=gharchive_partitions,
    description="GitHub Archive за 1 час → MinIO parquet (S3Queue → silver)",
)
def bronze_gharchive(
    context: AssetExecutionContext,
    minio: MinIOResource,
    clickhouse: ClickHouseResource,
) -> Output[dict]:
    target_dt = _partition_dt(context)
    context.log.info(f"GH Archive {target_dt:%Y-%m-%d %H:00} UTC")

    before = _scalar(
        clickhouse,
        "SELECT max(_loaded_at) FROM repo_radar.silver_github_events",
    )
    context.log.info(f"max(_loaded_at) до bronze: {before}")

    project_root = Path(__file__).parent.parent.parent
    extractor = GitHubArchiveExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        download_dir=Path("./data/downloads"),
        object_store=minio.get_store(),
    )

    result = extractor.process_hour(dt=target_dt, save_raw_sample=True)
    result["silver_max_loaded_at_before"] = before
    parquet_uri = result["parquet_uri"]

    return Output(
        value=result,
        metadata={
            "datetime": MetadataValue.text(target_dt.isoformat()),
            "events_count": MetadataValue.int(result["events_count"]),
            "parquet_uri": MetadataValue.text(parquet_uri or ""),
            "tracked_repos": MetadataValue.int(len(extractor.tracked_repos)),
            "silver_max_loaded_at_before": MetadataValue.text(str(before)),
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
) -> Output[int]:
    """Барьер после часа; skip, если events=0 / нет parquet."""
    if not bronze_gharchive.get("parquet_uri"):
        context.log.warning(
            "Пустой час (нет parquet) — S3Queue wait пропущен"
        )
        return Output(
            value=0,
            metadata={"skipped": MetadataValue.bool(True)},
        )
    return ensure_gharchive_hour(
        context,
        clickhouse,
        _partition_dt(context),
        )



