"""Локальный прогон П7 (Playwright): changelog → bronze → silver_changelogs."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from extractors.changelog.extractor_changelog import ChangelogExtractor
from extractors.clickhouse_loader import ClickHouseLoader
from resources.minio_resource import MinioStore

load_dotenv()


def main():
    print("\n" + "=" * 70)
    print("🚀 П7 Pipeline: changelog → bronze → silver_changelogs")
    print("=" * 70 + "\n")

    force = "--force" in sys.argv

    minio_store = MinioStore(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9002"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        bucket=os.getenv("MINIO_BUCKET", "bronze"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    extractor = ChangelogExtractor(
        sources_path=project_root / "config" / "changelog_sources.yml",
        bronze_dir=project_root / "data" / "bronze",
        checkpoint_dir=project_root / "data" / "checkpoints",
        object_store=minio_store,
    )

    result = extractor.process_snapshot(force=force)
    
    if not result["parquet_path"] or result["rows_count"] == 0:
        print("\n⚠️  Changelog-строк не найдено — silver_changelogs не обновлён")
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
    total = ch_loader.get_row_count("silver_changelogs")

    print(
        f"\n✅ П7 changelog готово: sources={result['parsed_sources']}, "
        f"loaded={rows_loaded}, count()={total}"
    )
    ch_loader.close()


if __name__ == "__main__":
    main()


