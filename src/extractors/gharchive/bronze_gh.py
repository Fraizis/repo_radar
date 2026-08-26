import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl


class GitHubArchiveBronzeWriter:
    def __init__(self, bronze_dir: Path, object_store=None):
        self.bronze_dir = bronze_dir
        self.object_store = object_store
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def save_to_parquet(self, events: list[dict], dt: datetime) -> Optional[Path]:
        if not events:
            print("   ⚠️  Нет событий для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        key = f"gharchive/dt={date_str}/hour={hour_str}/events.parquet"
        output_path = self.bronze_dir / key
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pl.DataFrame(events).write_parquet(output_path, compression="snappy")

        size_kb = output_path.stat().st_size / 1024
        print(f"   ✓ Сохранено {len(events)} событий → {output_path.name}")
        print(f"     Размер: {size_kb:.1f} KB")

        if self.object_store is not None:
            self.object_store.put_file(key, output_path)

        return output_path

    def save_raw_sample(self, events: list[dict], dt: datetime) -> Path:
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        key = f"gharchive_raw_sample/{date_str}-{hour_str}.json"
        output_path = self.bronze_dir / key
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2, ensure_ascii=False)

        if self.object_store is not None:
            self.object_store.put_file(key, output_path)

        return output_path


