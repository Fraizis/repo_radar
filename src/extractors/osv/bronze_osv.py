"""Запись снимка уязвимостей OSV в bronze.

Hive-путь: ``osv/dt=YYYY-MM-DD/advisories_<ts>.parquet``.
"""

from datetime import datetime

from extractors.bronze import daily_hive_key, save_snapshot_parquet
from resources.minio_resource import MinioStore


class OSVBronzeWriter:
    def __init__(self, object_store: MinioStore):
        self.object_store = object_store

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> str | None:
        return save_snapshot_parquet(
            self.object_store,
            rows,
            key=daily_hive_key("osv", "advisories", dt),
            empty_message="Нет уязвимостей для сохранения",
        )


