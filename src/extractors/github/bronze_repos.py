"""Запись снимка GitHub-репозиториев в bronze.

Hive-путь: ``github_repos/dt=YYYY-MM-DD/repos_<ts>.parquet``.
"""

from datetime import datetime

from extractors.bronze import daily_hive_key, save_snapshot_parquet
from resources.minio_resource import MinioStore


class GitHubReposBronzeWriter:
    def __init__(self, object_store: MinioStore):
        self.object_store = object_store

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> str | None:
        return save_snapshot_parquet(
            self.object_store,
            rows,
            key=daily_hive_key("github_repos", "repos", dt),
            empty_message="Нет репозиториев для сохранения",
        )


