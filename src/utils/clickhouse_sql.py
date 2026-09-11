from resources.clickhouse_resource import ClickHouseResource


def scalar(clickhouse: ClickHouseResource, sql: str):
    with clickhouse.get_client() as client:
        rows = client.query(sql).result_rows
    return rows[0][0] if rows else None

