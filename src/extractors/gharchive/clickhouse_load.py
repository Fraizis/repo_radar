"""Загрузка часа GH Archive в ClickHouse (fallback без S3Queue)."""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

from dagster import AssetExecutionContext, MetadataValue, Output

from resources.clickhouse_resource import ClickHouseResource
from utils.clickhouse_sql import scalar as _scalar

GHARCHIVE_PARQUET_STRUCTURE = (
    "event_id Nullable(String), event_type Nullable(String), event_time Nullable(String), "
    "actor_id Nullable(Int64), actor_login Nullable(String), repo_id Nullable(Int64), "
    "repo_name Nullable(String), public Nullable(UInt8), "
    "pr_action Nullable(String), pr_number Nullable(Int32), pr_merged Nullable(UInt8), "
    "issue_action Nullable(String), issue_number Nullable(Int32), "
    "push_size Nullable(Int32), push_ref Nullable(String), "
    "release_tag Nullable(String), release_name Nullable(String)"
)


def hour_count_sql(table: str, hour_start: datetime) -> str:
    """COUNT строк silver за ``[hour_start, hour_start+1h)`` по ``event_time``."""
    hour_end = hour_start + timedelta(hours=1)
    start_s = hour_start.strftime("%Y-%m-%d %H:%M:%S")
    end_s = hour_end.strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"SELECT count() FROM repo_radar.{table} "
        f"WHERE event_time >= toDateTime('{start_s}') "
        f"AND event_time < toDateTime('{end_s}')"
    )


def insert_gharchive_hour_from_s3(
    clickhouse: ClickHouseResource,
    hour_start: datetime,
) -> None:
    """Fallback: CH читает parquet с MinIO через ``s3()`` (как MV, без S3Queue)."""
    date_str = hour_start.strftime("%Y-%m-%d")
    hour_str = hour_start.strftime("%H")
    endpoint = os.getenv("CLICKHOUSE_S3_ENDPOINT", "http://minio:9000")
    user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    password = os.getenv("MINIO_ROOT_PASSWORD", "")
    url = f"{endpoint}/bronze/gharchive/dt={date_str}/hour={hour_str}/events.parquet"
    structure = GHARCHIVE_PARQUET_STRUCTURE
    sql = f"""
    INSERT INTO repo_radar.silver_github_events (
        event_id, event_type, event_time,
        actor_id, actor_login, repo_id, repo_name, public,
        pr_action, pr_number, pr_merged,
        issue_action, issue_number,
        push_size, push_ref, release_tag, release_name
    )
    SELECT
        coalesce(q.event_id, '') AS event_id,
        coalesce(q.event_type, '') AS event_type,
        parseDateTimeBestEffortOrNull(
            replaceRegexpOne(toString(q.event_time), '[Zz]$', '')
        ) AS event_time,
        coalesce(q.actor_id, 0) AS actor_id,
        coalesce(q.actor_login, '') AS actor_login,
        coalesce(q.repo_id, 0) AS repo_id,
        coalesce(q.repo_name, '') AS repo_name,
        toUInt8(ifNull(q.public, 1)) AS public,
        q.pr_action,
        q.pr_number,
        if(isNull(q.pr_merged), NULL, toUInt8(q.pr_merged)) AS pr_merged,
        q.issue_action,
        q.issue_number,
        q.push_size,
        q.push_ref,
        q.release_tag,
        q.release_name
    FROM s3('{url}', '{user}', '{password}', 'Parquet', '{structure}') AS q
    WHERE parseDateTimeBestEffortOrNull(
        replaceRegexpOne(toString(q.event_time), '[Zz]$', '')
    ) IS NOT NULL
    """
    with clickhouse.get_client() as client:
        client.command(sql)


def ensure_gharchive_hour(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    hour_start: datetime,
    *,
    attempts: int = 6,
    sleep_s: int = 5,
) -> Output[int]:
    """Ждёт S3Queue; если часа нет — INSERT FROM s3. Падает только если строк 0."""
    sql = hour_count_sql("silver_github_events", hour_start)
    for attempt in range(attempts):
        count = int(_scalar(clickhouse, sql) or 0)
        if count > 0:
            context.log.info(f"silver час на месте: rows={count}")
            return Output(value=count, metadata={"row_count": MetadataValue.int(count)})
        context.log.info(f"ожидание S3Queue {attempt + 1}/{attempts}")
        time.sleep(sleep_s)

    context.log.warning("S3Queue молчит — INSERT FROM s3()")
    insert_gharchive_hour_from_s3(clickhouse, hour_start)
    count = int(_scalar(clickhouse, sql) or 0)
    if count == 0:
        raise RuntimeError("После INSERT FROM s3 в silver всё ещё 0 строк за этот час")
    return Output(
        value=count,
        metadata={
            "row_count": MetadataValue.int(count),
            "loaded_via": MetadataValue.text("s3_insert"),
        },
    )
    

