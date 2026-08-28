"""Локальный прогон П5 без Dagster: GraphQL → bronze → silver_repos."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from extractors.clickhouse_loader import ClickHouseLoader
from extractors.github.extractor_repos import GitHubReposExtractor
from resources.minio_resource import MinioStore

load_dotenv()


def main():
    print("\n" + "=" * 70)
    print("🚀 П5 Pipeline: GitHub GraphQL → bronze → silver_repos")
    print("=" * 70 + "\n")

    token = os.getenv("GITHUB_TOKEN", "")
    if not token or token.startswith("ghp_your_token"):
        print("❌ Нужен реальный GITHUB_TOKEN в .env")
        return

    minio_store = MinioStore(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9002"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        bucket=os.getenv("MINIO_BUCKET", "bronze"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    extractor = GitHubReposExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        bronze_dir=project_root / "data" / "bronze",
        github_token=token,
        object_store=minio_store,
    )

    result = extractor.process_snapshot()
    if not result["parquet_path"] or result["repos_count"] == 0:
        print("\n❌ Пустой снимок")
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
    total = ch_loader.get_row_count("silver_repos")
    uniq = ch_loader.client.query(
        "SELECT uniqExact(repo_name) FROM repo_radar.silver_repos FINAL"
    ).result_rows[0][0]

    print(f"\n✅ П5 готово: loaded={rows_loaded}, count()={total}, uniq FINAL={uniq}")
    print(
    """
    SELECT language, count() AS repos, sum(stars) AS stars
    FROM repo_radar.silver_repos FINAL
    GROUP BY language
    ORDER BY repos DESC;
    """
    )
    ch_loader.close()


if __name__ == "__main__":
    main()

