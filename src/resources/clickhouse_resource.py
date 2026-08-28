"""
Dagster-ресурс подключения к ClickHouse (HTTP-интерфейс, порт 8123).
Не путать с ``ClickHouseLoader``: resource отдаёт сырой ``clickhouse_connect``
клиент и креды; loader умеет parquet → INSERT и DROP PARTITION.
"""

from dagster import ConfigurableResource
from pydantic import Field
import clickhouse_connect
from contextlib import contextmanager


class ClickHouseResource(ConfigurableResource):
    """Конфиг подключения к ClickHouse для assets.
    Значения по умолчанию совпадают с docker-compose; в Definitions
    перекрываются переменными ``CLICKHOUSE_*``.
    """

    host: str = Field(default="localhost", description="ClickHouse host")
    port: int = Field(default=8123, description="HTTP port")
    username: str = Field(default="default", description="Username")
    password: str = Field(default="", description="Password")
    database: str = Field(default="repo_radar", description="Database name")

    @contextmanager
    def get_client(self):
        """Контекстный менеджер: открыть HTTP-клиент и закрыть его на выходе.
        Yields:
            ``clickhouse_connect.driver.Client``.
        Note:
            Asset ``silver_github_events`` открывает клиент через этот CM,
            но INSERT идёт через отдельный ``ClickHouseLoader`` с теми же кредами.
            Клиент из ``yield`` в текущем коде не используется.
        """
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
        try:
            yield client
        finally:
            client.close()



