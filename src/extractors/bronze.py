"""Общая запись snapshot'ов в MinIO bronze (Hive-style parquet).

Доменные ``*BronzeWriter`` только задают prefix/имя файла, schema и
текст предупреждения; сама запись — здесь.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl

from resources.minio_resource import MinioStore

# polars schema alias: dict[str, DataType]
SchemaDict = dict[str, Any]


def daily_hive_key(prefix: str, filename_stem: str, dt: datetime) -> str:
    """``{prefix}/dt=YYYY-MM-DD/{filename_stem}_{unix_ts}.parquet``."""
    date_str = dt.strftime("%Y-%m-%d")
    ts = int(dt.timestamp())
    return f"{prefix}/dt={date_str}/{filename_stem}_{ts}.parquet"


def hourly_hive_key(prefix: str, filename: str, dt: datetime) -> str:
    """``{prefix}/dt=YYYY-MM-DD/hour=HH/{filename}`` (GH Archive)."""
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H")
    return f"{prefix}/dt={date_str}/hour={hour_str}/{filename}"


def save_snapshot_parquet(
    store: MinioStore,
    rows: list[dict],
    *,
    key: str,
    empty_message: str,
    schema: SchemaDict | None = None,
) -> str | None:
    """Пишет list[dict] → parquet в MinIO.

    Args:
        store: MinIO store.
        rows: Строки snapshot'а. Пустой список → warn + ``None`` (без записи).
        key: Object key в бакете.
        empty_message: Текст в консоль, если ``rows`` пуст.
        schema: Опциональная polars-схема (changelog, gharchive).

    Returns:
        ``s3://...`` URI или ``None``, если нечего писать.
    """
    if not rows:
        print(f"   ⚠️  {empty_message}")
        return None

    df = pl.DataFrame(rows, schema=schema) if schema is not None else pl.DataFrame(rows)
    return store.put_dataframe(key, df)

