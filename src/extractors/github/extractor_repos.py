"""
Оркестрация П5: seed YAML → GraphQL (dlt-source) → bronze parquet.
ClickHouse — отдельно через ``load_to_clickhouse``.
"""

from datetime import datetime, timezone
from pathlib import Path

from extractors.github.bronze_repos import GitHubReposBronzeWriter
from extractors.github.repos_source import github_repos_source, load_seed_repos


class GitHubReposExtractor:
    def __init__(
        self,
        tracked_repos_path: Path,
        bronze_dir: Path,
        github_token: str,
        object_store=None,
        batch_size: int = 50,
    ):
        self.tracked_repos_path = tracked_repos_path
        self.seed_repos = load_seed_repos(tracked_repos_path)
        self.github_token = github_token
        self.batch_size = batch_size
        self.bronze = GitHubReposBronzeWriter(bronze_dir, object_store=object_store)

    def process_snapshot(self, dt: datetime | None = None) -> dict:
        """Тянет все seed-репо и пишет parquet за дату ``dt`` (по умолчанию сегодня UTC)."""
        if dt is None:
            dt = datetime.now(timezone.utc).replace(tzinfo=None)

        print(f"\n{'=' * 60}")
        print(f"📦 GitHub GraphQL: снимок {dt.strftime('%Y-%m-%d')}")
        print(f"   seed: {len(self.seed_repos)} репозиториев")
        print(f"{'=' * 60}")

        source = github_repos_source(
            repos=self.seed_repos,
            github_token=self.github_token,
            batch_size=self.batch_size,
        )
        rows = list(source.repos)

        print(f"   ✓ Получено {len(rows)} репозиториев")
        parquet_path = self.bronze.save_to_parquet(rows, dt)

        return {
            "datetime": dt,
            "repos_count": len(rows),
            "seed_count": len(self.seed_repos),
            "parquet_path": parquet_path,
        }

    def load_to_clickhouse(self, parquet_path: Path, ch_loader) -> int:
        """INSERT в ``silver_repos``. Дедуп — ReplacingMergeTree, не DROP PARTITION."""
        return ch_loader.load_parquet_to_table(
            parquet_path=parquet_path,
            table_name="silver_repos",
            drop_partition=None,
            optimize_final=True,
        )

