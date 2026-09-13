"""Общие HTTP-дефолты клиентов repo-radar."""

from __future__ import annotations

DEFAULT_MAX_RETRIES = 5


def user_agent(component: str) -> str:
    """``repo-radar/0.1 ({component})``."""
    return f"repo-radar/0.1 ({component})"

