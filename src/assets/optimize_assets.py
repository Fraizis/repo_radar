"""Maintenance: OPTIMIZE FINAL для snapshot silver (ReplacingMergeTree)."""

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from resources.clickhouse_resource import ClickHouseResource

DB = "repo_radar"

# Только снимки без дневных партиций (см. TODO шаг 9).
# silver_github_events — отдельно, если понадобится partition-wise OPTIMIZE.
SNAPSHOT_TABLES = (
    "silver_repos",
    "silver_advisories",
    "silver_changelogs",
)


@asset(
    group_name="maintenance",
    description=(
        "OPTIMIZE TABLE … FINAL для silver_repos / silver_advisories / "
        "silver_changelogs (дедуп ReplacingMergeTree)"
    ),
)
def optimize_silver_snapshots(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> Output[dict]:
    results: dict[str, int] = {}

    with clickhouse.get_client() as client:
        for table in SNAPSHOT_TABLES:
            sql = f"OPTIMIZE TABLE {DB}.{table} FINAL"
            context.log.info(sql)
            client.command(sql)
            # После FINAL count() ≈ уникальные строки по ORDER BY
            count = int(
                client.query(f"SELECT count() FROM {DB}.{table}").result_rows[0][0]
            )
            results[table] = count
            context.log.info(f"{table}: rows after OPTIMIZE = {count}")

    return Output(
        value=results,
        metadata={
            f"{table}_rows": MetadataValue.int(n) for table, n in results.items()
        },
    )

