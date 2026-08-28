"""Локальный прогон П7 (OSV): OSV API → bronze → silver_advisories."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from extractors.clickhouse_loader import ClickHouseLoader
from extractors.osv.extractor_osv import OSVExtractor
from resources.minio_resource import MinioStore

load_dotenv()


def main():
    print("\n" + "=" * 70)
    print("🚀 П7 Pipeline: OSV → bronze → silver_advisories")
    print("=" * 70 + "\n")

    minio_store = MinioStore(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9002"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        bucket=os.getenv("MINIO_BUCKET", "bronze"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    extractor = OSVExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        bronze_dir=project_root / "data" / "bronze",
        object_store=minio_store,
    )

    result = extractor.process_snapshot()
    if not result["parquet_path"] or result["advisories_count"] == 0:
        print("\n⚠️  Уязвимостей не найдено — silver_advisories не обновлён")
        return

    ch_loader = ClickHouseLoader(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database="repo_radar",
    )
    if not ch_loader.test_connection():
        print("\n❌ ClickHouse недоступен (make up)")
        return

    rows_loaded = extractor.load_to_clickhouse(result["parquet_path"], ch_loader)
    total = ch_loader.get_row_count("silver_advisories")

    print(f"\n✅ П7 OSV готово: loaded={rows_loaded}, count()={total}")
    ch_loader.close()


if __name__ == "__main__":
    main()

