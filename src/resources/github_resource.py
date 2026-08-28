"""Dagster-ресурс: GitHub token из env."""

from dagster import ConfigurableResource
from pydantic import Field


class GitHubResource(ConfigurableResource):
    token: str = Field(default="", description="GITHUB_TOKEN")

    def require_token(self) -> str:
        if not self.token:
            raise ValueError("GITHUB_TOKEN не задан — положи его в .env")
        return self.token

