"""
Оркестрация П7 (Playwright): changelog_sources.yml → скрейп → bronze parquet.
ClickHouse — отдельно через load_to_clickhouse (ReplacingMergeTree).
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from extractors.changelog.bronze_changelog import ChangelogBronzeWriter
from extractors.changelog.changelog_client import ChangelogScraper, load_changelog_sources
from resources.minio_resource import MinioStore


class ChangelogExtractor:
    def __init__(
        self,
        sources_path: Path,
        object_store: MinioStore,
        checkpoint_dir: Path,
        skip_recent_hours: int = 24,
    ):
        self.sources = load_changelog_sources(sources_path)
        self.bronze = ChangelogBronzeWriter(object_store, checkpoint_dir)
        self.skip_recent_hours = skip_recent_hours

    def _is_recent(self, checkpoint: dict, url: str) -> bool:
        """True, если URL уже скрейпили менее skip_recent_hours назад."""
        entry = checkpoint.get(url)
        if not entry:
            return False
        try:
            last = datetime.fromisoformat(entry["scraped_at"])
        except (KeyError, ValueError):
            return False
        return datetime.now(UTC) - last < timedelta(hours=self.skip_recent_hours)

    def process_snapshot(self, dt: datetime | None = None, force: bool = False) -> dict:
        if dt is None:
            dt = datetime.now(UTC).replace(tzinfo=None)

        print(f"\n{'=' * 60}")
        print(f"📰 Changelogs: снимок {dt.strftime('%Y-%m-%d')}")
        print(f"   источников: {len(self.sources)}")
        print(f"{'=' * 60}")

        checkpoint = self.bronze.load_checkpoint()
        all_rows: list[dict] = []
        parsed_sources = 0
        scraped_urls: list[str] = []

        with ChangelogScraper() as scraper:
            for src in self.sources:
                url = src.get("url")
                name = src.get("name") or "unknown"
                if not url:
                    continue
                if not force and self._is_recent(checkpoint, url):
                    print(f"   ⏭️  '{name}' скрейпили недавно — пропуск (force=False)")
                    continue

                rows = scraper.scrape(src)
                scraped_urls.append(url)
                if rows:
                    all_rows.extend(rows)
                    parsed_sources += 1

        print(f"   ✓ Спарсено источников: {parsed_sources}, строк всего: {len(all_rows)}")
        parquet_uri = self.bronze.save_to_parquet(all_rows, dt)

        if parquet_uri is not None:
            counts: dict[str, int] = {}
            for r in all_rows:
                counts[r["url"]] = counts.get(r["url"], 0) + 1
            for url in scraped_urls:
                self.bronze.update_checkpoint(checkpoint, url, counts.get(url, 0))

        return {
            "datetime": dt,
            "rows_count": len(all_rows),
            "parsed_sources": parsed_sources,
            "parquet_uri": parquet_uri,
        }

