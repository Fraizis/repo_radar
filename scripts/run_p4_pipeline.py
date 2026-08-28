"""
Скрипт для тестирования П4 pipeline:
GitHub Archive → bronze → silver_github_events
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from extractors.gharchive.extractor_gh import GitHubArchiveExtractor
from extractors.clickhouse_loader import ClickHouseLoader
from resources.minio_resource import MinioStore

load_dotenv()


def main():
    """Локальный прогон П4 без Dagster: GH Archive → bronze parquet → ClickHouse.
    Берёт вчера 12:00, качает час, фильтрует seed, пишет parquet (+ MinIO),
    затем ``DROP PARTITION`` + INSERT в ``silver_github_events``.
    Нужны поднятые MinIO и ClickHouse (``make up``) и ``tracked_repos.yml``.
    При 0 событий после фильтра или ошибке коннекта к CH — ранний ``return``.
    В конце печатает проверочный SQL по ``event_type`` за этот день.
    """
    
    print("\n" + "=" * 70)
    print("🚀 П4 Pipeline: GitHub Archive → bronze → silver_github_events")
    print("=" * 70 + "\n")

    # Параметры
    target_dt = datetime.now() - timedelta(days=1)
    target_dt = target_dt.replace(hour=12, minute=0, second=0, microsecond=0)

    # Шаг 1: GitHub Archive → bronze (Parquet)
    print("📦 ШАГ 1: Извлечение GitHub Archive → bronze layer\n")


    minio_store = MinioStore(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9002"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        bucket=os.getenv("MINIO_BUCKET", "bronze"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )

    extractor = GitHubArchiveExtractor(
        tracked_repos_path=project_root / "config" / "tracked_repos.yml",
        download_dir=project_root / "data" / "downloads",
        bronze_dir=project_root / "data" / "bronze",
        object_store=minio_store,
        )

    result = extractor.process_hour(
        dt=target_dt,
        save_raw_sample=True,
        )

    if result['events_count'] == 0:
        print("\n❌ Не найдено событий для загрузки")
        return

    # Шаг 2: bronze → silver (ClickHouse)
    print(f"\n\n📊 ШАГ 2: Загрузка в ClickHouse silver layer\n")

    ch_loader = ClickHouseLoader(
        host="localhost",
        port=8123,
        username="default",
        password="clickhouse123",
        database="repo_radar",
    )

    # Проверка подключения
    if not ch_loader.test_connection():
        print("\n❌ Не удалось подключиться к ClickHouse")
        print("   Убедитесь, что сервисы запущены: make up")
        return

    print("   ✓ Подключение к ClickHouse установлено")

    # Загрузка данных
    rows_loaded = extractor.load_to_clickhouse(
        parquet_path=result['parquet_path'],
        dt=target_dt,
        ch_loader=ch_loader,
    )

    # Проверка результата
    total_rows = ch_loader.get_row_count("silver_github_events")

    print(f"\n{'=' * 70}")
    print("✅ П4 Pipeline завершён успешно!")
    print("=" * 70)
    print(f"📅 Дата/час:           {target_dt.strftime('%Y-%m-%d %H:00')}")
    print(f"📊 Событий обработано: {result['events_count']}")
    print(f"💾 Загружено в CH:     {rows_loaded} строк")
    print(f"📈 Всего в таблице:    {total_rows} строк")
    print(f"📁 Parquet:            {result['parquet_path']}")
    if result['sample_path']:
        print(f"📄 Raw sample:         {result['sample_path']}")
    print("=" * 70 + "\n")

    # SQL запрос для проверки
    print("🔍 Проверочный SQL запрос:\n")
    print(f"""
    SELECT 
        event_type,
        count() as cnt,
        uniq(repo_name) as repos
    FROM repo_radar.silver_github_events
    WHERE toYYYYMMDD(event_time) = {target_dt.strftime('%Y%m%d')}
    GROUP BY event_type
    ORDER BY cnt DESC;
    """)

    ch_loader.close()


if __name__ == "__main__":
    main()


