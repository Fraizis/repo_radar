"""Asset dbt_build: CH silver_* → stg/gold через `dbt build`.
Sources в transform/models/staging/_sources.yml (`silver.<table>`) читают
таблицы ClickHouse. В lineage Dagster они должны висеть на python-барьерах
`*_ready` (после S3Queue), а не на несуществующих AssetKey(["silver_*"]).
"""

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from config.paths import TRANSFORM_DIR

dbt_project = DbtProject(project_dir=TRANSFORM_DIR)
dbt_project.prepare_if_dev()


_SOURCE_TO_UPSTREAM: dict[str, str] = {
    "silver_github_events": "silver_github_events_ready",
    "silver_repos": "silver_repos_ready",
    "silver_advisories": "silver_advisories_ready",
    "silver_changelogs": "silver_changelogs_ready",
}


class SilverSourceTranslator(DagsterDbtTranslator):
    """Source `silver.<table>` → AssetKey соответствующего `*_ready`."""

    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            name = dbt_resource_props["name"]
            upstream = _SOURCE_TO_UPSTREAM.get(name, name)
            return AssetKey([upstream])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SilverSourceTranslator(),
)
def dbt_build(context: AssetExecutionContext, dbt: DbtCliResource):
    """`dbt build` — staging + gold модели и тесты."""
    yield from dbt.cli(["build"], context=context).stream()


