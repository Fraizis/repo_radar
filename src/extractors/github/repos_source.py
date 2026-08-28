"""
dlt-source: GitHub GraphQL → строки репозиториев из seed.
Не пишет в destination сам: экстрактор итерирует resource и кладёт parquet.
"""

from pathlib import Path

import dlt
import yaml
from extractors.github.graphql_client import GitHubGraphQLClient



def load_seed_repos(tracked_repos_path: Path) -> list[dict]:
    """Читает ``config/tracked_repos.yml`` как список dict (owner/name/...).
    В отличие от ``events_gh.load_tracked_repos`` (там set ``owner/name``),
    здесь нужны отдельные поля — GraphQL принимает owner и name по отдельности.
    """
    with open(tracked_repos_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return list(data.get("repositories") or [])


@dlt.source(name="github_graphql")
def github_repos_source(
    repos: list[dict],
    github_token: str,
    batch_size: int = 50,
):
    """Source с одним resource ``repos``.
    Args:
        repos: Seed из YAML.
        github_token: PAT / fine-grained token (для public repo хватает metadata).
        batch_size: Размер GraphQL-батча.
    """

    @dlt.resource(
        name="repos",
        write_disposition="merge",
        primary_key="repo_name",
    )
    def repos_resource():
        client = GitHubGraphQLClient(token=github_token, batch_size=batch_size)
        try:
            for row in client.fetch_repos(repos):
                yield row
        finally:
            client.close()

    return repos_resource

