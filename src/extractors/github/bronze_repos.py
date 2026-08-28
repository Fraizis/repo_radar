"""
Запись снимка GitHub-репозиториев в bronze.
Hive-путь: ``github_repos/dt=YYYY-MM-DD/repos.parquet``.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl


class GitHubReposBronzeWriter:
    def __init__(self, bronze_dir: Path, object_store=None):
        self.bronze_dir = bronze_dir
        self.object_store = object_store   
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def save_to_parquet(self, rows: list[dict], dt: datetime) -> Optional[Path]:
        """Запись списка репозиториев в parquet и зеркалирование.
        Args:
            rows: Список словарей с репозиториями.
            dt: Дата для имени партиции.
        Returns:
            Путь к сохранённому файлу или ``None``, если нет репозиториев.
        """
        if not rows:
            print("   ⚠️  Нет репозиториев для сохранения")
            return None

        date_str = dt.strftime("%Y-%m-%d")
        key = f"github_repos/dt={date_str}/repos.parquet"
        output_path = self.bronze_dir / key
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pl.DataFrame(rows).write_parquet(output_path, compression="snappy")
        size_kb = output_path.stat().st_size / 1024
        print(f"   ✓ Сохранено {len(rows)} репо → {output_path.name} ({size_kb:.1f} KB)")

        if self.object_store is not None:
            self.object_store.put_file(key, output_path)

        return output_path

