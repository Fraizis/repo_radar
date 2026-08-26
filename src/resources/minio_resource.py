from pathlib import Path

from dagster import ConfigurableResource
from minio import Minio
from pydantic import Field


class MinioStore:
    """S3-клиент MinIO: бакет bronze, put локального файла."""

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
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def put_file(self, key: str, local_path: Path) -> str:
        self.ensure_bucket()
        self.client.fput_object(self.bucket, key, str(local_path))
        uri = f"s3://{self.bucket}/{key}"
        print(f"   ✓ MinIO ← {uri}")
        return uri


class MinIOResource(ConfigurableResource):
    endpoint: str = Field(default="localhost:9002")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin123")
    bucket: str = Field(default="bronze")
    secure: bool = Field(default=False)
    bronze_path: str = Field(default="./data/bronze")

    def get_bronze_path(self) -> Path:
        path = Path(self.bronze_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_store(self) -> MinioStore:
        return MinioStore(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            bucket=self.bucket,
            secure=self.secure,
        )


