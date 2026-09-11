"""Incoming Webhook в Slack. Пустой URL = no-op (локалка без алертов)."""

import httpx
from dagster import ConfigurableResource
from pydantic import Field


class SlackResource(ConfigurableResource):
    webhook_url: str = Field(default="", description="SLACK_WEBHOOK_URL")
    dagster_base_url: str = Field(
        default="http://localhost:3001",
        description="Ссылки на run в UI",
    )

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def post(
        self,
        text: str,
        blocks: list[dict] | None = None,
        *,
        color: str | None = None,
        ) -> None:
        if not self.enabled:
            return

        if color:
            payload = {
                "attachments": [
                    {
                        "color": color,
                        "fallback": text,
                        "text": text,
                        "mrkdwn_in": ["text"],
                    }
                ]
            }
        elif blocks:
            payload = {"text": text, "blocks": blocks}
        else:
            payload = {"text": text}

        with httpx.Client(timeout=15.0) as client:
            resp = client.post(self.webhook_url, json=payload)
            resp.raise_for_status()


    def run_url(self, run_id: str) -> str:
        return f"{self.dagster_base_url.rstrip('/')}/runs/{run_id}"


