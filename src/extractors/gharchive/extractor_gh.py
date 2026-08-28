"""
Оркестрация часа GitHub Archive: download → filter → transform → bronze parquet.
Склеивает ``GitHubArchiveDownloader``, ``events_gh`` и ``GitHubArchiveBronzeWriter``.
Загрузку в ClickHouse делает отдельно через ``load_to_clickhouse``.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from extractors.gharchive.bronze_gh import GitHubArchiveBronzeWriter
from extractors.gharchive.download_gh import GitHubArchiveDownloader
from extractors.gharchive.events_gh import (
    filter_events,
    load_tracked_repos,
    read_events,
    transform_event,
)
from extractors.gharchive.progress_gh import ProgressCallback

RAW_SAMPLE_SIZE = 100


class GitHubArchiveExtractor:
    """Скачать час GH Archive → фильтр seed → parquet bronze.
    При создании сразу читает ``tracked_repos.yml`` в память.
    Args:
        tracked_repos_path: YAML со списком репозиториев.
        download_dir: Куда класть ``.json.gz``.
        bronze_dir: Корень bronze (локальный диск).
        object_store: Опциональный MinIO-клиент для зеркалирования parquet/sample.
    """
    def __init__(
        self,
        tracked_repos_path: Path,
        download_dir: Path,
        bronze_dir: Path,
        object_store=None,
    ):
        self.tracked_repos_path = tracked_repos_path
        self.tracked_repos = load_tracked_repos(tracked_repos_path)
        self.downloader = GitHubArchiveDownloader(download_dir)
        self.bronze = GitHubArchiveBronzeWriter(bronze_dir, object_store=object_store)

    def process_hour(
        self,
        dt: datetime,
        save_raw_sample: bool = True,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> dict:
        """Полный цикл обработки одного часа архива.
        Шаги:
            1. Скачать (или переиспользовать) ``.json.gz``.
            2. Прочитать события, попутно набрав raw sample.
            3. Отфильтровать по seed и типам, трансформировать.
            4. Записать parquet; опционально JSON-sample.
        Args:
            dt: Час для обработки (обычно вчера 12:00 UTC-локально, как в assets).
            save_raw_sample: Писать ли JSON-sample первых ``RAW_SAMPLE_SIZE`` событий.
            progress_callback: Прогресс скачивания; ``None`` → консольный бар.
        Returns:
            Словарь:
                * ``datetime`` — исходный ``dt``
                * ``events_count`` — число строк после фильтра
                * ``parquet_path`` — путь к parquet или ``None``, если событий 0
                * ``sample_path`` — путь к sample или ``None``
        """

        print(f"\n{'=' * 60}")
        print(f"📦 GitHub Archive: {dt.strftime('%Y-%m-%d %H:00')}")
        print(f"{'=' * 60}")

        archive_path = self.downloader.download_archive(
            dt, progress_callback=progress_callback
        )

        print(
            f"   🔍 Фильтруем события "
            f"(отслеживаем {len(self.tracked_repos)} репозиториев)..."
        )

        raw_sample: list[dict] = []

        def events_with_sample():
            for event in read_events(archive_path):
                if len(raw_sample) < RAW_SAMPLE_SIZE:
                    raw_sample.append(event)
                yield event

        transformed = []
        for event in filter_events(
            events_with_sample(),
            tracked_repos=self.tracked_repos,
        ):
            transformed.append(transform_event(event))

        print(f"   ✓ Отфильтровано {len(transformed)} событий")

        parquet_path = self.bronze.save_to_parquet(transformed, dt)

        sample_path = None
        if save_raw_sample and raw_sample:
            sample_path = self.bronze.save_raw_sample(raw_sample, dt)
            print(f"   ✓ Сохранён raw sample ({len(raw_sample)} событий) → {sample_path.name}")

        return {
            "datetime": dt,
            "events_count": len(transformed),
            "parquet_path": parquet_path,
            "sample_path": sample_path,
        }

    def load_to_clickhouse(self, parquet_path: Path, dt: datetime, ch_loader) -> int:
        """Загружает parquet часа в ``silver_github_events`` с заменой партиции.
        Партиция — ``YYYYMMDD`` из ``dt``. Перед INSERT выполняется
        ``DROP PARTITION``, поэтому повторный прогон идемпотентен на уровне дня
        (не часа: вся дневная партиция перезаписывается этим файлом).
        Args:
            parquet_path: Файл, который вернул ``process_hour``.
            dt: Тот же час, что обрабатывали (для id партиции).
            ch_loader: Экземпляр ``ClickHouseLoader``.
        Returns:
            Число вставленных строк.
        """ 
        partition_id = dt.strftime("%Y%m%d")
        return ch_loader.load_parquet_to_table(
            parquet_path=parquet_path,
            table_name="silver_github_events",
            drop_partition=partition_id,
        )


