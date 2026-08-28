"""
Запись changelog'ов в bronze + checkpoint по URL.
Hive-путь: changelogs/dt=YYYY-MM-DD/changelogs.parquet.
Checkpoint: data/checkpoints/changelogs.json — {url: {scraped_at, versions}}.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import polars as pl


class ChangelogBronzeWriter:
    def __init__(self, bronze_dir: Path, checkpoint_dir: Path, object_store=None):
        self.bronze_dir = bronze_dir
        self.object_store = object_store
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = checkpoint_dir / "changelogs.json"

    # --- checkpoint ---------------------------------------------------------
    def load_checkpoint(self) -> dict:
        """Читает чекпоинт {url: {...}}; пустой dict, если файла нет."""
        if not self.checkpoint_path.exists():
            return {}
        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            return json.load(f) or {}

    def update_checkpoint(self, checkpoint: dict, url: str, versions: int) -> None:
        """Обновляет запись по URL и сразу пишет на диск (устойчиво к падению)."""
        checkpoint[url] = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "versions": versions,
        }
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    # --- bronze -------------------------------------------------------------
    def save_to_parquet(self, rows: list[dict], dt: datetime) -> Optional[Path]:
        if not rows:
            print("   ⚠️  Нет changelog-строк для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        key = f"changelogs/dt={date_str}/changelogs.parquet"
        output_path = self.bronze_dir / key
        output_path.parent.mkdir(parents=True, exist_ok=True)
        schema = {
            "source": pl.String,
            "url": pl.String,
            "version": pl.String,
            "heading": pl.String,
            "release_date": pl.Datetime("us"),
            "scraped_at": pl.Datetime("us"),
            }
        pl.DataFrame(rows, schema=schema).write_parquet(output_path, compression="snappy")

        size_kb = output_path.stat().st_size / 1024
        print(f"   ✓ Сохранено {len(rows)} строк → {output_path.name} ({size_kb:.1f} KB)")

        if self.object_store is not None:
            self.object_store.put_file(key, output_path)

        return output_path

