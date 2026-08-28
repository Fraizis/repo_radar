"""
Оркестрация П7 (OSV): seed YAML → OSV API → bronze parquet.
ClickHouse — отдельно через ``load_to_clickhouse`` (ReplacingMergeTree).
"""

from datetime import datetime, timezone
from pathlib import Path

from extractors.github.repos_source import load_seed_repos
from extractors.osv.bronze_osv import OSVBronzeWriter
from extractors.osv.osv_client import OSVClient


class OSVExtractor:
    def __init__(
        self,
        tracked_repos_path: Path,
        bronze_dir: Path,
        object_store=None,
        batch_size: int = 100,
    ):
        self.seed_repos = load_seed_repos(tracked_repos_path)
        self.batch_size = batch_size
        self.bronze = OSVBronzeWriter(bronze_dir, object_store=object_store)

    def process_snapshot(self, dt: datetime | None = None) -> dict:
        if dt is None:
            dt = datetime.now(timezone.utc).replace(tzinfo=None)

        print(f"\n{'=' * 60}")
        print(f"🔒 OSV: снимок уязвимостей {dt.strftime('%Y-%m-%d')}")
        print(f"   seed: {len(self.seed_repos)} репозиториев")
        print(f"{'=' * 60}")

        client = OSVClient(batch_size=self.batch_size)
        try:
            rows = client.fetch_advisories(self.seed_repos)
        finally:
            client.close()

        print(f"   ✓ Всего строк advisories: {len(rows)}")
        parquet_path = self.bronze.save_to_parquet(rows, dt)

        return {
            "datetime": dt,
            "advisories_count": len(rows),
            "seed_count": len(self.seed_repos),
            "parquet_path": parquet_path,
        }

    def load_to_clickhouse(self, parquet_path: Path, ch_loader) -> int:
        """INSERT в ``silver_advisories``. Дедуп — ReplacingMergeTree, не DROP PARTITION."""
        return ch_loader.load_parquet_to_table(
            parquet_path=parquet_path,
            table_name="silver_advisories",
            drop_partition=None,
            optimize_final=True,
        )

