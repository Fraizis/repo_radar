"""
dlt-source: GitHub GraphQL → строки репозиториев из seed.
Не пишет в destination сам: экстрактор итерирует resource и кладёт parquet.
"""

import dlt

from extractors.github.graphql_client import GitHubGraphQLClient


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
            yield from client.fetch_repos(repos)
        finally:
            client.close()

    return repos_resource

