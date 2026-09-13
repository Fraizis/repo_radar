"""Партиции Dagster для GH Archive (hourly UTC)."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, HourlyPartitionsDefinition


def gharchive_start_date() -> str:
    """Старт партиций: ``GHARCHIVE_START_DATE`` или now−3d (час)."""
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
    start_date=gharchive_start_date(),
    timezone="UTC",
    end_offset=0,
)


def partition_dt(context: AssetExecutionContext) -> datetime:
    """UTC-naive datetime часа партиции (ключ ``YYYY-MM-DD-HH:MM``)."""
    return datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")

