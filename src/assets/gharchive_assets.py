"""
Assets для GitHub Archive pipeline
"""
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from extractors.gharchive.extractor_gh import GitHubArchiveExtractor
from extractors.clickhouse_loader import ClickHouseLoader 
from resources.clickhouse_resource import ClickHouseResource
from resources.minio_resource import MinIOResource

@asset(
group_name="github_archive",
description="Скачивание и фильтрация GitHub Archive → bronze layer (Parquet в MinIO)",
)
def bronze_gharchive(
    context: AssetExecutionContext,
    minio: MinIOResource,
    ) -> Output[dict]:
    """
    Asset: Извлечение событий из GitHub Archive

    Действия:
    1. Скачать hourly архив .json.gz
    2. Фильтровать по tracked_repos и типам событий
    3. Трансформировать в плоскую структуру
    4. Сохранить в Parquet (bronze/gharchive/dt=YYYY-MM-DD/hour=HH/)
    5. Сохранить один raw sample для документации

    Returns:
        dict: Метаданные (datetime, events_count, parquet_path)
    """

    # Дата для обработки (по умолчанию — вчера в 12:00)
    # В production это будет параметризоваться через партиции
    target_dt = datetime.now() - timedelta(days=1)
    target_dt = target_dt.replace(hour=12, minute=0, second=0, microsecond=0)

    context.log.info(f"Обработка GitHub Archive за {target_dt.strftime('%Y-%m-%d %H:00')}")

    # Инициализация экстрактора
    project_root = Path(__file__).parent.parent.parent
    extractor = GitHubArchiveExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        download_dir=Path("./data/downloads"),
        bronze_dir=minio.get_bronze_path(),
        object_store=minio.get_store(),
        )

    result = extractor.process_hour(
        dt=target_dt,
        save_raw_sample=True,
    )

    context.log.info(f"✓ Обработано {result['events_count']} событий")
    context.log.info(f"✓ Parquet: {result['parquet_path']}")

    return Output(
        value=result,
        metadata={
            "datetime": MetadataValue.text(result['datetime'].isoformat()),
            "events_count": MetadataValue.int(result['events_count']),
            "parquet_path": MetadataValue.path(str(result['parquet_path'])),
            "tracked_repos": MetadataValue.int(len(extractor.tracked_repos)),
        },
    )


@asset(
group_name="github_archive",
description="Загрузка событий из bronze (Parquet) → silver_github_events (ClickHouse)",
deps=[bronze_gharchive],  # Зависимость от предыдущего asset
)
def silver_github_events(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    bronze_gharchive: dict,
    ) -> Output[int]:
    """
    Asset: Загрузка данных в ClickHouse silver layer

    Действия:
    1. Прочитать Parquet из bronze
    2. Удалить существующую партицию (идемпотентность)
    3. Загрузить данные в silver_github_events
    4. Проверить количество строк

    Returns:
        int: Количество загруженных строк
    """

    parquet_path = Path(bronze_gharchive['parquet_path'])
    dt = bronze_gharchive['datetime']

    context.log.info(f"Загрузка {parquet_path} в ClickHouse...")

    # Инициализация загрузчика
    with clickhouse.get_client() as client:
        loader = ClickHouseLoader(
            host=clickhouse.host,
            port=clickhouse.port,
            username=clickhouse.username,
            password=clickhouse.password,
            database=clickhouse.database,
        )

        # Загрузка с удалением партиции
        partition_id = dt.strftime("%Y%m%d")
        rows_loaded = loader.load_parquet_to_table(
            parquet_path=parquet_path,
            table_name="silver_github_events",
            drop_partition=partition_id,
        )

        # Проверка общего количества строк
        total_rows = loader.get_row_count("silver_github_events")

        context.log.info(f"✓ Загружено {rows_loaded} строк")
        context.log.info(f"✓ Всего в таблице: {total_rows} строк")

    return Output(
        value=rows_loaded,
        metadata={
            "rows_loaded": MetadataValue.int(rows_loaded),
            "total_rows": MetadataValue.int(total_rows),
            "partition_id": MetadataValue.text(partition_id),
            "table": MetadataValue.text("silver_github_events"),
        },
    )



