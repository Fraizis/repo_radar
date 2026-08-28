"""Assets П5: GitHub GraphQL → bronze → silver_repos."""

from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from extractors.clickhouse_loader import ClickHouseLoader
from extractors.github.extractor_repos import GitHubReposExtractor
from resources.clickhouse_resource import ClickHouseResource
from resources.github_resource import GitHubResource
from resources.minio_resource import MinIOResource


@asset(
    group_name="github_repos",
    description="GitHub GraphQL → bronze (Parquet в MinIO)",
)
def bronze_github_repos(
    context: AssetExecutionContext,
    minio: MinIOResource,
    github: GitHubResource,
) -> Output[dict]:
    project_root = Path(__file__).parent.parent.parent
    extractor = GitHubReposExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        bronze_dir=minio.get_bronze_path(),
        github_token=github.require_token(),
        object_store=minio.get_store(),
    )
    result = extractor.process_snapshot()
    if not result["parquet_path"]:
        raise RuntimeError("Пустой снимок GraphQL — parquet не записан")
        
    context.log.info(f"✓ Снимок: {result['repos_count']} репо")

    return Output(
        value=result,
        metadata={
            "repos_count": MetadataValue.int(result["repos_count"]),
            "seed_count": MetadataValue.int(result["seed_count"]),
            "parquet_path": MetadataValue.path(str(result["parquet_path"])),
        },
    )


@asset(
    group_name="github_repos",
    description="bronze parquet → silver_repos (ClickHouse)",
)
def silver_repos(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    bronze_github_repos: dict,
) -> Output[int]:
    parquet_path = Path(bronze_github_repos["parquet_path"])

    with clickhouse.get_client() as _client:
        loader = ClickHouseLoader(
            host=clickhouse.host,
            port=clickhouse.port,
            username=clickhouse.username,
            password=clickhouse.password,
            database=clickhouse.database,
        )
        rows_loaded = loader.load_parquet_to_table(
            parquet_path=parquet_path,
            table_name="silver_repos",
            optimize_final=True,
        )
        total_rows = loader.get_row_count("silver_repos")
        loader.close()

    context.log.info(f"✓ Загружено {rows_loaded}, в таблице {total_rows}")

    return Output(
        value=rows_loaded,
        metadata={
            "rows_loaded": MetadataValue.int(rows_loaded),
            "total_rows": MetadataValue.int(total_rows),
            "table": MetadataValue.text("silver_repos"),
            },
        )


