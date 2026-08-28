"""
Оркестрация П7 (Playwright): changelog_sources.yml → скрейп → bronze parquet.
ClickHouse — отдельно через load_to_clickhouse (ReplacingMergeTree).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from extractors.changelog.bronze_changelog import ChangelogBronzeWriter
from extractors.changelog.changelog_client import ChangelogScraper, load_changelog_sources


class ChangelogExtractor:
    def __init__(
        self,
        sources_path: Path,
        bronze_dir: Path,
        checkpoint_dir: Path,
        object_store=None,
        skip_recent_hours: int = 24,
    ):
        self.sources = load_changelog_sources(sources_path)
        self.bronze = ChangelogBronzeWriter(bronze_dir, checkpoint_dir, object_store=object_store)
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
        return datetime.now(timezone.utc) - last < timedelta(hours=self.skip_recent_hours)

    def process_snapshot(self, dt: datetime | None = None, force: bool = False) -> dict:
        if dt is None:
            dt = datetime.now(timezone.utc).replace(tzinfo=None)

        print(f"\n{'=' * 60}")
        print(f"📰 Changelogs: снимок {dt.strftime('%Y-%m-%d')}")
        print(f"   источников: {len(self.sources)}")
        print(f"{'=' * 60}")

        checkpoint = self.bronze.load_checkpoint()
        all_rows: list[dict] = []
        parsed_sources = 0
        scraped_urls: list[str] = []  # URL, реально обойдённые в этом прогоне

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
        parquet_path = self.bronze.save_to_parquet(all_rows, dt)

        # checkpoint пишем ТОЛЬКО после успешной записи parquet,
        # иначе падение персиста «залочит» источники на skip_recent_hours
        if parquet_path is not None:
            counts: dict[str, int] = {}
            for r in all_rows:
                counts[r["url"]] = counts.get(r["url"], 0) + 1
            for url in scraped_urls:
                self.bronze.update_checkpoint(checkpoint, url, counts.get(url, 0))

        return {
            "datetime": dt,
            "rows_count": len(all_rows),
            "parsed_sources": parsed_sources,
            "parquet_path": parquet_path,
        }
    

    def load_to_clickhouse(self, parquet_path: Path, ch_loader) -> int:
        """INSERT в silver_changelogs. Дедуп — ReplacingMergeTree, не DROP PARTITION."""
        return ch_loader.load_parquet_to_table(
            parquet_path=parquet_path,
            table_name="silver_changelogs",
            drop_partition=None,
            optimize_final=True,
        )


