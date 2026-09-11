"""
Запись снимка уязвимостей OSV в bronze.
Hive-путь: ``osv/dt=YYYY-MM-DD/advisories_<ts>.parquet``.
"""

from datetime import datetime

import polars as pl

from resources.minio_resource import MinioStore


class OSVBronzeWriter:
    def __init__(self, object_store: MinioStore):
        self.object_store = object_store

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> str | None:
        if not rows:
            print("   ⚠️  Нет уязвимостей для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        ts = int(dt.timestamp())
        key = f"osv/dt={date_str}/advisories_{ts}.parquet"
        df = pl.DataFrame(rows)
        return self.object_store.put_dataframe(key, df)


