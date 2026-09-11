"""
Оркестрация часа GitHub Archive: download → filter → transform → bronze parquet.
Склеивает ``GitHubArchiveDownloader``, ``events_gh`` и ``GitHubArchiveBronzeWriter``.
Загрузку в ClickHouse делает отдельно через ``load_to_clickhouse``.
"""

from datetime import datetime
from pathlib import Path

from extractors.gharchive.bronze_gh import GitHubArchiveBronzeWriter
from extractors.gharchive.download_gh import GitHubArchiveDownloader
from extractors.gharchive.events_gh import (
    filter_events,
    load_tracked_repos,
    read_events,
    transform_event,
)
from extractors.gharchive.progress_gh import ProgressCallback
from resources.minio_resource import MinioStore

RAW_SAMPLE_SIZE = 100


class GitHubArchiveExtractor:
    def __init__(
        self,
        tracked_repos_path: Path,
        download_dir: Path,
        object_store: MinioStore,
    ):
        self.tracked_repos_path = tracked_repos_path
        self.tracked_repos = load_tracked_repos(tracked_repos_path)
        self.downloader = GitHubArchiveDownloader(download_dir)
        self.bronze = GitHubArchiveBronzeWriter(object_store)

    def process_hour(
        self,
        dt: datetime,
        save_raw_sample: bool = True,
        progress_callback: ProgressCallback | None = None,
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
                * ``parquet_uri`` — URI к parquet или ``None``, если событий 0
                * ``sample_uri`` — URI к sample или ``None``
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

        parquet_uri = self.bronze.save_to_parquet(transformed, dt)
        sample_uri = None

        if save_raw_sample and raw_sample:
            sample_uri = self.bronze.save_raw_sample(raw_sample, dt)
        return {
            "datetime": dt,
            "events_count": len(transformed),
            "parquet_uri": parquet_uri,
            "sample_uri": sample_uri,
        }

