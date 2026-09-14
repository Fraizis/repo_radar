from datetime import UTC, datetime
from pathlib import Path

from extractors.osv.bronze_osv import OSVBronzeWriter
from extractors.osv.osv_client import OSVClient
from resources.minio_resource import MinioStore

from config.tracked_repos import load_repositories


class OSVExtractor:
    def __init__(
        self,
        tracked_repos_path: Path,
        object_store: MinioStore,
        batch_size: int = 100,
    ):
        self.seed_repos = load_repositories(tracked_repos_path)
        self.batch_size = batch_size
        self.bronze = OSVBronzeWriter(object_store)

    def process_snapshot(self, dt: datetime | None = None) -> dict:
        if dt is None:
            dt = datetime.now(UTC).replace(tzinfo=None)

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

        parquet_uri = self.bronze.save_to_parquet(rows, dt)
        return {
            "datetime": dt,
            "advisories_count": len(rows),
            "seed_count": len(self.seed_repos),
            "parquet_uri": parquet_uri,
        }

