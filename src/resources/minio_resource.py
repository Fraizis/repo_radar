from __future__ import annotations

import io
from pathlib import Path

import polars as pl
from dagster import ConfigurableResource
from minio import Minio
from minio.error import S3Error
from pydantic import Field


class MinioStore:
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

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self.ensure_bucket()
        data_stream = io.BytesIO(data)
        self.client.put_object(
            self.bucket,
            key,
            data_stream,
            length=len(data),
            content_type=content_type,
        )
        uri = f"s3://{self.bucket}/{key}"
        print(f"   ✓ MinIO ← {uri}")
        return uri

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def get_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error:
            return False

    def put_dataframe(self, key: str, df: pl.DataFrame) -> str:
        buf = io.BytesIO()
        df.write_parquet(buf, compression="snappy")
        buf.seek(0)
        return self.put_bytes(key, buf.getvalue(), content_type="application/octet-stream")

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
    endpoint: str = Field(default="localhost:9002")
    access_key: str = Field(default="")
    secret_key: str = Field(default="")
    bucket: str = Field(default="bronze")
    secure: bool = Field(default=False)

    def get_store(self) -> MinioStore:
        """Собирает ``MinioStore`` из полей ресурса.
        Returns:
            Клиент для ``put_file`` из bronze-writer.
        """
        if not self.access_key or not self.secret_key:
            raise ValueError("MINIO_ROOT_USER / MINIO_ROOT_PASSWORD не заданы")

        return MinioStore(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            bucket=self.bucket,
            secure=self.secure,
        )


