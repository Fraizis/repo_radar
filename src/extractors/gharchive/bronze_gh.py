"""
Запись отфильтрованных событий GitHub Archive в bronze-слой.
Партиции Hive-style: ``gharchive/dt=YYYY-MM-DD/hour=HH/events.parquet``.
"""

import json
from datetime import datetime

import polars as pl

from resources.minio_resource import MinioStore

GHARCHIVE_SCHEMA = {
    "event_id": pl.Utf8,
    "event_type": pl.Utf8,
    "event_time": pl.Utf8,
    "actor_id": pl.Int64,
    "actor_login": pl.Utf8,
    "repo_id": pl.Int64,
    "repo_name": pl.Utf8,
    "public": pl.Boolean,
    "pr_action": pl.Utf8,
    "pr_number": pl.Int32,
    "pr_merged": pl.Boolean,
    "issue_action": pl.Utf8,
    "issue_number": pl.Int32,
    "push_size": pl.Int32,
    "push_ref": pl.Utf8,
    "release_tag": pl.Utf8,
    "release_name": pl.Utf8,
}


class GitHubArchiveBronzeWriter:
    def __init__(self, object_store: MinioStore):
        self.object_store = object_store

    def save_to_parquet(self, events: list[dict], dt: datetime) -> str | None:
        if not events:
            print("   ⚠️  Нет событий для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        key = f"gharchive/dt={date_str}/hour={hour_str}/events.parquet"
        df = pl.DataFrame(events, schema=GHARCHIVE_SCHEMA)
        return self.object_store.put_dataframe(key, df)


    def save_raw_sample(self, events: list[dict], dt: datetime) -> str:
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        key = f"gharchive_raw_sample/{date_str}-{hour_str}.json"
        payload = json.dumps(events, indent=2, ensure_ascii=False).encode("utf-8")
        return self.object_store.put_bytes(key, payload, content_type="application/json")


