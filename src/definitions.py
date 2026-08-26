"""
Dagster Definitions — точка входа для оркестратора
"""
from dagster import Definitions, EnvVar
from pathlib import Path
import os

from assets.gharchive_assets import bronze_gharchive, silver_github_events
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource

defs = Definitions(
    assets=[
        bronze_gharchive,
        silver_github_events,
    ],
    resources={
        "clickhouse": ClickHouseResource(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD","clickhouse123"),
            database="repo_radar",
        ),
        "minio": MinIOResource(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9002"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
            bucket=os.getenv("MINIO_BUCKET", "bronze"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            bronze_path=os.getenv("BRONZE_PATH", "./data/bronze"),
        ),
    },
)



