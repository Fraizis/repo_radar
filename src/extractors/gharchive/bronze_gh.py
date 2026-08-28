"""
Запись отфильтрованных событий GitHub Archive в bronze-слой.
Партиции Hive-style: ``gharchive/dt=YYYY-MM-DD/hour=HH/events.parquet``.
Опционально дублирует файлы в object store (MinIO) через ``put_file``.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl


class GitHubArchiveBronzeWriter:
    """Пишет parquet и JSON-sample в локальный bronze-каталог и, при наличии, в MinIO.
    Args:
        bronze_dir: Корневой каталог bronze (например ``./data/bronze``).
            Создаётся при инициализации, если его ещё нет.
        object_store: Клиент хранилища с методом ``put_file(key, local_path)``.
            Обычно ``MinioStore``. Если ``None``, запись только на диск.
    """
    def __init__(self, bronze_dir: Path, object_store=None):
        self.bronze_dir = bronze_dir
        self.object_store = object_store
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def save_to_parquet(self, events: list[dict], dt: datetime) -> Optional[Path]:
        """Сохраняет список событий в Snappy-Parquet за указанный час.
        Путь: ``{bronze_dir}/gharchive/dt={YYYY-MM-DD}/hour={HH}/events.parquet``.
        Если передан ``object_store``, тот же ключ загружается в бакет.
        Args:
            events: Плоские словари после ``transform_event``. Пустой список
                не создаёт файл.
            dt: Час партиции (используются дата и час, минуты игнорируются).
        Returns:
            Путь к локальному parquet или ``None``, если ``events`` пуст.
        """
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
        """Пишет JSON-sample сырых событий для отладки и документации схемы.
        Путь: ``{bronze_dir}/gharchive_raw_sample/{YYYY-MM-DD}-{HH}.json``.
        Args:
            events: Сырые объекты GH Archive (до transform), обычно первые 100.
            dt: Час, к которому относится sample.
        Returns:
            Путь к локальному JSON-файлу.
        """
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


