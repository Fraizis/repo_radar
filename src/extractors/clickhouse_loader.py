"""
Загрузка данных в ClickHouse
"""

import clickhouse_connect
from pathlib import Path
import polars as pl
from typing import Optional


class ClickHouseLoader:
    """
    Загрузчик данных в ClickHouse из Parquet
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "repo_radar",
    ):
        """
        Args:
            host: Хост ClickHouse
            port: HTTP порт (обычно 8123)
            username: Имя пользователя
            password: Пароль
            database: База данных
        """
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        self.database = database

    @staticmethod
    def _to_naive_utc(col: str, dtype: pl.DataType) -> pl.Expr:
        """Любой ISO datetime → naive UTC под колонку ClickHouse DateTime.

        GraphQL/parquet могут принести:
        * ``2024-01-15T10:30:00Z``
        * ``2024-01-15T10:30:00+00:00``
        * ``2024-01-15T10:30:00.123Z``
        * уже Datetime (если распарсили в graphql-клиенте)
        ``strict=False``: битая/пустая строка → null, а не exception.
        """
        if dtype == pl.String:
            return (
                pl.col(col)
                .str.replace(r"[Zz]$", "+00:00")
                .str.to_datetime(strict=False)
                .dt.replace_time_zone(None)
            )
        return pl.col(col).dt.replace_time_zone(None)

    @staticmethod  
    def _prepare_silver_repos(df: pl.DataFrame) -> pl.DataFrame:
        """Parquet → типы ``silver_repos``.
        GraphQL отдаёт даты naive UTC; stars/forks — int; description может быть null.
        """
        exprs = []

        if "repo_id" in df.columns:
            exprs.append(pl.col("repo_id").fill_null(0).cast(pl.UInt64))
        for col in ("repo_name", "owner", "name", "ecosystem", "package"):
            if col in df.columns:
                exprs.append(pl.col(col).fill_null("").cast(pl.String))
        if "description" in df.columns:
            exprs.append(pl.col("description").cast(pl.String))
        if "language" in df.columns:
            exprs.append(pl.col("language").cast(pl.String))
        if "stars" in df.columns:
            exprs.append(pl.col("stars").fill_null(0).cast(pl.UInt64))
        if "forks" in df.columns:
            exprs.append(pl.col("forks").fill_null(0).cast(pl.UInt64)) 

        for col in ("pushed_at", "updated_at", "created_at"):
            if col not in df.columns:
                continue
            exprs.append(ClickHouseLoader._to_naive_utc(col, df.schema[col]))
            
        return df.with_columns(exprs) if exprs else df

    @staticmethod 
    def _prepare_silver_advisories(df: pl.DataFrame) -> pl.DataFrame:
        """Parquet → типы ``silver_advisories``.
        Строковые поля с fill_null(''); опциональные — просто cast(String);
        даты published/modified → naive UTC через общий ``_to_naive_utc``.
        """
        exprs = []

        for col in ("vuln_id", "repo_name", "package"):
            if col in df.columns:
                exprs.append(pl.col(col).fill_null("").cast(pl.String))
        if "ecosystem" in df.columns:
            exprs.append(pl.col("ecosystem").fill_null("unknown").cast(pl.String))
        if "severity" in df.columns:
            exprs.append(pl.col("severity").fill_null("UNKNOWN").cast(pl.String))
        for col in ("cve_id", "aliases", "summary", "cvss_vector"):
            if col in df.columns:
                exprs.append(pl.col(col).cast(pl.String))

        for col in ("published", "modified"):
            if col in df.columns:
                exprs.append(ClickHouseLoader._to_naive_utc(col, df.schema[col]))

        return df.with_columns(exprs) if exprs else df

    @staticmethod
    def _prepare_silver_changelogs(df: pl.DataFrame) -> pl.DataFrame:
        """Parquet → типы silver_changelogs.
        Строковые ключи с fill_null(''); heading опционален;
        release_date/scraped_at → naive UTC через общий _to_naive_utc."""
        exprs = []

        for col in ("source", "url", "version"):
            if col in df.columns:
                exprs.append(pl.col(col).fill_null("").cast(pl.String))
        if "heading" in df.columns:
            exprs.append(pl.col("heading").cast(pl.String))

        for col in ("release_date", "scraped_at"):
            if col in df.columns:
                exprs.append(ClickHouseLoader._to_naive_utc(col, df.schema[col]))

        return df.with_columns(exprs) if exprs else df

    @staticmethod 
    def _prepare_silver_github_events(df: pl.DataFrame) -> pl.DataFrame:
        """Приводит parquet-колонки к типам таблицы ``silver_github_events``.
        Нужен, потому что Polars из parquet даёт более широкие типы, чем CH:
        ``event_time`` — ISO-строка ``YYYY-MM-DDTHH:MM:SSZ`` → naive datetime;
        id-поля — Int с null → UInt + fill_null; ``public`` / ``pr_merged`` → UInt8.
        Колонки, которых нет в DataFrame, пропускаются (sample/частичный файл).
        ``strict=False`` на опциональных полях: невалидные значения → null, а не exception.
        Args:
            df: DataFrame после ``pl.read_parquet``.
        Returns:
            Тот же df с ``with_columns``; без изменений, если ни одной известной колонки нет.
        """
        exprs = []

        if "event_id" in df.columns:
            exprs.append(pl.col("event_id").cast(pl.String))
        if "event_type" in df.columns:
            exprs.append(pl.col("event_type").cast(pl.String))
        if "event_time" in df.columns:
            exprs.append(
                pl.col("event_time")
                .str.to_datetime("%Y-%m-%dT%H:%M:%SZ")
                .dt.replace_time_zone(None)
            )
        if "actor_id" in df.columns:
            exprs.append(pl.col("actor_id").fill_null(0).cast(pl.UInt64))
        if "actor_login" in df.columns:
            exprs.append(pl.col("actor_login").fill_null("").cast(pl.String))
        if "repo_id" in df.columns:
            exprs.append(pl.col("repo_id").fill_null(0).cast(pl.UInt64))
        if "repo_name" in df.columns:
            exprs.append(pl.col("repo_name").fill_null("").cast(pl.String))
        if "public" in df.columns:
            exprs.append(pl.col("public").fill_null(1).cast(pl.UInt8))

        if "pr_action" in df.columns:
            exprs.append(pl.col("pr_action").cast(pl.String))
        if "pr_number" in df.columns:
            exprs.append(pl.col("pr_number").cast(pl.UInt32, strict=False))
        if "pr_merged" in df.columns:
            exprs.append(pl.col("pr_merged").cast(pl.UInt8, strict=False))

        if "issue_action" in df.columns:
            exprs.append(pl.col("issue_action").cast(pl.String))
        if "issue_number" in df.columns:
            exprs.append(pl.col("issue_number").cast(pl.UInt32, strict=False))

        if "push_size" in df.columns:
            exprs.append(pl.col("push_size").cast(pl.UInt32, strict=False))
        if "push_ref" in df.columns:
            exprs.append(pl.col("push_ref").cast(pl.String))

        if "release_tag" in df.columns:
            exprs.append(pl.col("release_tag").cast(pl.String))
        if "release_name" in df.columns:
            exprs.append(pl.col("release_name").cast(pl.String))

        return df.with_columns(exprs) if exprs else df

    def load_parquet_to_table(
        self,
        parquet_path: Path,
        table_name: str,
        drop_partition: Optional[str] = None,
        optimize_final: bool = False
    ) -> int:
        """
        Загрузить Parquet файл в таблицу ClickHouse
        
        Args:
            parquet_path: Путь к Parquet файлу
            table_name: Имя таблицы
            drop_partition: Значение партиции для удаления перед вставкой (YYYYMMDD)
        
        Returns:
            int: Количество загруженных строк
        """
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet файл не найден: {parquet_path}")
        
        df = pl.read_parquet(parquet_path)
        
        if df.height == 0:
            print(f"   ⚠️  Parquet пустой: {parquet_path}")
            return 0

        if table_name == "silver_github_events":
            df = self._prepare_silver_github_events(df)
        elif table_name == "silver_repos":
            df = self._prepare_silver_repos(df)
        elif table_name == "silver_advisories": 
            df = self._prepare_silver_advisories(df)
        elif table_name == "silver_changelogs":
            df = self._prepare_silver_changelogs(df)

        if drop_partition:
            partition_id = drop_partition
            print(f"   🗑️  Удаляем партицию: {partition_id}")
            self.client.command(
                f"ALTER TABLE {self.database}.{table_name} DROP PARTITION {partition_id}"
            )
                
        print(f"   ⬆️  Загружаем {df.height} строк в {table_name}...")
        
        self.client.insert(
            table=table_name,
            data=df.rows(),
            column_names=list(df.columns),
        )

        if optimize_final:
            print(f"   🔧 OPTIMIZE TABLE {table_name} FINAL")
            self.client.command(
                f"OPTIMIZE TABLE {self.database}.{table_name} FINAL"
            )
        
        print(f"   ✓ Загружено {df.height} строк")
        
        return df.height
    
    def get_row_count(self, table_name: str) -> int:
        """
        Получить количество строк в таблице
        
        Args:
            table_name: Имя таблицы
        
        Returns:
            int: Количество строк
        """
        result = self.client.query(f"SELECT count() FROM {self.database}.{table_name}")
        return result.result_rows[0][0]
    
    def test_connection(self) -> bool:
        """
        Проверить подключение к ClickHouse
        
        Returns:
            bool: True если подключение успешно
        """
        try:
            result = self.client.query("SELECT 1")
            return result.result_rows[0][0] == 1
        except Exception as e:
            print(f"   ✗ Ошибка подключения к ClickHouse: {e}")
            return False
    
    def close(self):
        """Закрыть подключение"""
        self.client.close()



