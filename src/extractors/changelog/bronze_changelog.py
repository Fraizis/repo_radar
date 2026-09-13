"""Запись changelog'ов в bronze.

Hive-путь: changelogs/dt=YYYY-MM-DD/changelogs_<ts>.parquet.
"""

from datetime import datetime

import polars as pl

from extractors.bronze import daily_hive_key, save_snapshot_parquet
from resources.minio_resource import MinioStore

CHANGELOG_SCHEMA = {
    "source": pl.String,
    "url": pl.String,
    "version": pl.String,
    "heading": pl.String,
    "release_date": pl.Datetime("us"),
    "scraped_at": pl.Datetime("us"),
}


class ChangelogBronzeWriter:
    def __init__(self, object_store: MinioStore):
        self.object_store = object_store

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> str | None:
        return save_snapshot_parquet(
            self.object_store,
            rows,
            key=daily_hive_key("changelogs", "changelogs", dt),
            empty_message="Нет changelog-строк для сохранения",
            schema=CHANGELOG_SCHEMA,
        )

