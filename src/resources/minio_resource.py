"""
MinIO / S3 resource для bronze-слоя.
Два уровня:
    * ``MinioStore`` — низкоуровневый клиент (``minio.Minio``): бакет + ``put_file``.
    * ``MinIOResource`` — Dagster ConfigurableResource: креды из env и локальный
      путь bronze, который используют assets.
"""

from pathlib import Path

from dagster import ConfigurableResource
from minio import Minio
from pydantic import Field


class MinioStore:
    """S3-клиент MinIO: бакет bronze, загрузка локального файла по ключу.
    Args:
        endpoint: Хост:порт API, без схемы (например ``localhost:9002``).
        access_key: Access key (``MINIO_ROOT_USER``).
        secret_key: Secret key (``MINIO_ROOT_PASSWORD``).
        bucket: Имя бакета, по умолчанию ``bronze``.
        secure: ``True`` для HTTPS. Локальный docker-compose обычно ``False``.
    """
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "bronze",
        secure: bool = False,
    ):
        self.bucket = bucket
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket(self) -> None:
        """Создаёт бакет, если его ещё нет. Идемпотентно."""
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def put_file(self, key: str, local_path: Path) -> str:
        """Загружает локальный файл в бакет.
        Args:
            key: Object key, например ``gharchive/dt=2026-08-26/hour=12/events.parquet``.
            local_path: Существующий файл на диске.
        Returns:
            URI вида ``s3://{bucket}/{key}``.
        """
        self.ensure_bucket()
        self.client.fput_object(self.bucket, key, str(local_path))
        uri = f"s3://{self.bucket}/{key}"
        print(f"   ✓ MinIO ← {uri}")
        return uri


class MinIOResource(ConfigurableResource):
    """Dagster-ресурс: креды MinIO + локальный каталог bronze.
    Поля читаются из Definitions / env (см. ``src/definitions.py``).
    Assets берут диск через ``get_bronze_path()`` и S3 через ``get_store()``.
    """

    endpoint: str = Field(default="localhost:9002")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin123")
    bucket: str = Field(default="bronze")
    secure: bool = Field(default=False)
    bronze_path: str = Field(default="./data/bronze")

    def get_bronze_path(self) -> Path:
        """Локальный корень bronze. Создаёт каталог, если его нет.
        Returns:
            ``Path`` к ``bronze_path``.
        """
        path = Path(self.bronze_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_store(self) -> MinioStore:
        """Собирает ``MinioStore`` из полей ресурса.
        Returns:
            Клиент для ``put_file`` из bronze-writer.
        """
        return MinioStore(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            bucket=self.bucket,
            secure=self.secure,
        )


