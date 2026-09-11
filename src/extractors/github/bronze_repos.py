"""
Запись снимка GitHub-репозиториев в bronze.
Hive-путь: ``github_repos/dt=YYYY-MM-DD/repos_<ts>.parquet``.
"""

from datetime import datetime

import polars as pl

from resources.minio_resource import MinioStore


class GitHubReposBronzeWriter:
    def __init__(self, object_store: MinioStore):
        self.object_store = object_store

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> str | None:
        if not rows:
            print("   ⚠️  Нет репозиториев для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        ts = int(dt.timestamp())
        key = f"github_repos/dt={date_str}/repos_{ts}.parquet"
        return self.object_store.put_dataframe(key, pl.DataFrame(rows))


