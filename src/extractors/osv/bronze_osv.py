"""
Запись снимка уязвимостей OSV в bronze.
Hive-путь: ``osv/dt=YYYY-MM-DD/advisories.parquet``.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl


class OSVBronzeWriter:
    def __init__(self, bronze_dir: Path, object_store=None):
        self.bronze_dir = bronze_dir
        self.object_store = object_store
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> Optional[Path]:
        if not rows:
            print("   ⚠️  Нет уязвимостей для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        key = f"osv/dt={date_str}/advisories.parquet"
        output_path = self.bronze_dir / key
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pl.DataFrame(rows).write_parquet(output_path, compression="snappy")
        size_kb = output_path.stat().st_size / 1024
        print(f"   ✓ Сохранено {len(rows)} advisories → {output_path.name} ({size_kb:.1f} KB)")

        if self.object_store is not None:
            self.object_store.put_file(key, output_path)

        return output_path

