"""Алерты в Slack: FAILURE, failed checks, короткий SUCCESS (+ mini-report для full_pipeline)."""

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)

from resources.clickhouse_resource import ClickHouseResource
from resources.slack_resource import SlackResource

DB = "repo_radar"


def _meta_value(mat, key: str):
    entry = mat.metadata.get(key)
    return None if entry is None else entry.value


def _failed_check_lines(context: RunStatusSensorContext) -> list[str]:
    """Собирает failed asset checks без фильтра of_type (он иногда пустой)."""
    failed: list[str] = []
    for log in context.instance.all_logs(context.dagster_run.run_id):
        de = log.dagster_event
        if de is None or de.event_type != DagsterEventType.ASSET_CHECK_EVALUATION:
            continue
        ev = de.event_specific_data
        if ev is None or getattr(ev, "passed", True):
            continue

        key = getattr(ev, "asset_check_key", None)
        if key is not None:
            label = f"{key.asset_key.to_user_string()} / {key.name}"
        else:
            label = f"{ev.asset_key.to_user_string()} / {ev.check_name}"

        sev = getattr(ev.severity, "value", str(ev.severity))
        failed.append(f"• `{label}` ({sev})")
    return failed


def _post_success(
    context: RunStatusSensorContext,
    slack: SlackResource,
    summary_lines: list[str] | None = None,
) -> None:
    run = context.dagster_run
    url = slack.run_url(run.run_id)
    partition = run.tags.get("dagster/partition")
    title = f":star: `{run.job_name}` SUCCESS"
    if partition:
        title += f" `{partition}`"

    body = [title]
    if summary_lines:
        body.extend(summary_lines)

    failed = _failed_check_lines(context)
    if failed:
        body.append(f":warning: {len(failed)} check(s) failed:")
        body.extend(failed[:10])

    body.append(f"<{url}|Open in Dagster>")
    text = "\n".join(body)
    slack.post(text, color="warning" if failed else "good")


def _post_gharchive_hour(context: RunStatusSensorContext, slack: SlackResource) -> None:
    events = None
    skipped = False

    for log in context.instance.all_logs(
        context.dagster_run.run_id,
        of_type=DagsterEventType.ASSET_MATERIALIZATION,
    ):
        data = log.dagster_event.event_specific_data
        if data is None:
            continue
        mat = data.materialization
        name = mat.asset_key.to_user_string()
        if name == "bronze_gharchive":
            raw = _meta_value(mat, "events_count")
            events = int(raw) if raw is not None else None
        if name == "silver_github_events_ready":
            skipped = bool(_meta_value(mat, "skipped") or False)

    if skipped or events == 0:
        return

    _post_success(context, slack, summary_lines=[f"events: *{events}*"])


def _short_full_pipeline_report(clickhouse: ClickHouseResource) -> list[str]:
    """Короткая сводка: счётчики + топ-3, без длинных description."""
    lines: list[str] = []
    try:
        with clickhouse.get_client() as client:
            n_cve = client.query(
                f"""
                SELECT count()
                FROM {DB}.gold_cve_exposure
                WHERE upperUTF8(ifNull(severity, '')) IN ('CRITICAL', 'HIGH')
                  AND published >= now() - INTERVAL 2 DAY
                """
            ).result_rows[0][0]
            top_cve = client.query(
                f"""
                SELECT repo_name, cve_id, severity
                FROM {DB}.gold_cve_exposure
                WHERE upperUTF8(ifNull(severity, '')) IN ('CRITICAL', 'HIGH')
                  AND published >= now() - INTERVAL 2 DAY
                ORDER BY published DESC
                LIMIT 3
                """
            ).result_rows
            top_rising = client.query(
                f"""
                SELECT repo_name, growth_score
                FROM {DB}.gold_rising_repos
                ORDER BY growth_score DESC
                LIMIT 3
                """
            ).result_rows
    except Exception:
        return ["report: gold недоступен"]

    lines.append(f"CVE CRITICAL/HIGH (2d): *{n_cve}*")
    for repo, cve_id, sev in top_cve:
        lines.append(f"• `{repo}` {cve_id or '—'} {sev}")
    if top_rising:
        lines.append("Rising top-3:")
        for repo, score in top_rising:
            lines.append(f"• `{repo}` score={score}")
    return lines


@run_failure_sensor(
    name="slack_on_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_on_run_failure(
    context: RunFailureSensorContext,
    slack: SlackResource,
) -> None:
    if not slack.enabled:
        return

    run = context.dagster_run
    err = (context.failure_event.message or "unknown error")[:800]
    url = slack.run_url(run.run_id)
    text = (
        f":x: `{run.job_name}` FAILED\n"
        f"*Run:* `{run.run_id}`\n"
        f"```{err}```\n"
        f"<{url}|Open in Dagster>"
    )
    slack.post(text, color="danger")


@run_status_sensor(
    name="slack_on_job_success",
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_on_job_success(
    context: RunStatusSensorContext,
    slack: SlackResource,
    clickhouse: ClickHouseResource,
) -> None:
    if not slack.enabled:
        return

    job = context.dagster_run.job_name

    if job == "gharchive_job":
        _post_gharchive_hour(context, slack)
        return
    if job == "full_pipeline_job":
        _post_success(context, slack, summary_lines=_short_full_pipeline_report(clickhouse))
        return
    if job in ("repos_osv_job", "changelogs_job"):
        _post_success(context, slack)




