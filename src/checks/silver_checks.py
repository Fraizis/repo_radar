"""Asset checks: проверки качества silver-таблиц.

Вешаются на `*_ready` (после ingest в CH), не на bronze —
иначе freshness/volume видят старый max(_loaded_at) до S3Queue/INSERT.

  * row_count_positive — в таблице есть строки (ERROR, blocking).
  * freshness — max(_loaded_at) не старше max_age_hours (WARN).
  * volume_anomaly — дневной объём vs медиана (WARN); только events.
"""



import statistics

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    MetadataValue,
    asset_check,
)

from assets.changelog_assets import silver_changelogs_ready
from assets.gharchive_assets import silver_github_events_ready
from assets.github_repos_assets import silver_repos_ready
from assets.osv_assets import silver_advisories_ready
from resources.clickhouse_resource import ClickHouseResource
from utils.clickhouse_sql import scalar as _scalar

DB = "repo_radar"


def build_row_count_check(asset_def, table: str) -> AssetChecksDefinition:
    @asset_check(
        asset=asset_def,
        name="row_count_positive",
        blocking=True,
        description=f"{table}: количество строк > 0",
    )
    def _check(clickhouse: ClickHouseResource) -> AssetCheckResult:
        count = int(_scalar(clickhouse, f"SELECT count() FROM {DB}.{table}") or 0)
        return AssetCheckResult(
            passed=count > 0,
            severity=AssetCheckSeverity.ERROR,
            metadata={"row_count": MetadataValue.int(count)},
        )

    return _check


def build_freshness_check(
    asset_def, table: str, max_age_hours: int
) -> AssetChecksDefinition:
    @asset_check(
        asset=asset_def,
        name="freshness",
        blocking=False,
        description=f"{table}: max(_loaded_at) не старше {max_age_hours} ч",
    )
    def _check(clickhouse: ClickHouseResource) -> AssetCheckResult:
        count = int(_scalar(clickhouse, f"SELECT count() FROM {DB}.{table}") or 0)
        if count == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                metadata={"reason": MetadataValue.text("нет данных для оценки freshness")},
            )
        age = int(
            _scalar(
                clickhouse,
                f"SELECT dateDiff('hour', max(_loaded_at), now()) FROM {DB}.{table}",
            )
        )
        return AssetCheckResult(
            passed=age <= max_age_hours,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "age_hours": MetadataValue.int(age),
                "max_age_hours": MetadataValue.int(max_age_hours),
            },
        )

    return _check


def build_volume_anomaly_check(
    asset_def,
    table: str,
    time_col: str,
    deviation_factor: float = 3.0,
    min_days: int = 3,
) -> AssetChecksDefinition:
    @asset_check(
        asset=asset_def,
        name="volume_anomaly",
        blocking=False,
        description=f"{table}: дневной объём в пределах x{deviation_factor} от медианы",
    )
    def _check(clickhouse: ClickHouseResource) -> AssetCheckResult:
        with clickhouse.get_client() as client:
            rows = client.query(
                f"SELECT toDate({time_col}) AS d, count() AS c "
                f"FROM {DB}.{table} GROUP BY d ORDER BY d"
            ).result_rows

        if len(rows) < min_days:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "reason": MetadataValue.text(
                        f"мало дней для оценки: {len(rows)} < {min_days}"
                    )
                },
            )

        counts = [int(r[1]) for r in rows]
        latest = counts[-1]
        median_prev = statistics.median(counts[:-1])
        low = median_prev / deviation_factor
        high = median_prev * deviation_factor
        anomaly = latest < low or latest > high

        return AssetCheckResult(
            passed=not anomaly,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "latest_day_count": MetadataValue.int(latest),
                "median_prev_days": MetadataValue.float(float(median_prev)),
                "lower_bound": MetadataValue.float(float(low)),
                "upper_bound": MetadataValue.float(float(high)),
                "days_observed": MetadataValue.int(len(rows)),
            },
        )

    return _check


all_silver_checks: list[AssetChecksDefinition] = [
    build_row_count_check(silver_github_events_ready, "silver_github_events"),
    build_freshness_check(silver_github_events_ready, "silver_github_events", max_age_hours=24),
    build_volume_anomaly_check(
        silver_github_events_ready, "silver_github_events", "event_time"
    ),
    build_row_count_check(silver_repos_ready, "silver_repos"),
    build_freshness_check(silver_repos_ready, "silver_repos", max_age_hours=24),
    build_row_count_check(silver_advisories_ready, "silver_advisories"),
    build_freshness_check(silver_advisories_ready, "silver_advisories", max_age_hours=24),
    build_row_count_check(silver_changelogs_ready, "silver_changelogs"),
    build_freshness_check(silver_changelogs_ready, "silver_changelogs", max_age_hours=48),
]


