"""
Запись changelog'ов в bronze + checkpoint по URL.
Hive-путь: changelogs/dt=YYYY-MM-DD/changelogs_<ts>.parquet.
Checkpoint: data/checkpoints/changelogs.json — {url: {scraped_at, versions}}.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from resources.minio_resource import MinioStore


class ChangelogBronzeWriter:
    def __init__(self, object_store: MinioStore, checkpoint_dir: Path):
        self.object_store = object_store
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = checkpoint_dir / "changelogs.json"

    def load_checkpoint(self) -> dict:
        """Читает чекпоинт {url: {...}}; пустой dict, если файла нет."""
        if not self.checkpoint_path.exists():
            return {}
        with open(self.checkpoint_path, encoding="utf-8") as f:
            return json.load(f) or {}

    def update_checkpoint(self, checkpoint: dict, url: str, versions: int) -> None:
        """Обновляет запись по URL и сразу пишет на диск (устойчиво к падению)."""
        checkpoint[url] = {
            "scraped_at": datetime.now(UTC).isoformat(),
            "versions": versions,
        }
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)


    def save_to_parquet(self, rows: list[dict], dt: datetime) -> str | None:
        if not rows:
            print("   ⚠️  Нет changelog-строк для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        ts = int(dt.timestamp())
        key = f"changelogs/dt={date_str}/changelogs_{ts}.parquet"
        schema = {
            "source": pl.String,
            "url": pl.String,
            "version": pl.String,
            "heading": pl.String,
            "release_date": pl.Datetime("us"),
            "scraped_at": pl.Datetime("us"),
        }
        return self.object_store.put_dataframe(key, pl.DataFrame(rows, schema=schema))

