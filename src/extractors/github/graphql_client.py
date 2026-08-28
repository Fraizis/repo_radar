"""
GitHub GraphQL клиент: метаданные репозиториев батчами.
Эндпоинт ``https://api.github.com/graphql``. Лимит — points/час
(для простого repository-запроса cost ≈ 1). Батч aliases = одна round-trip.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Iterator, Optional

import httpx

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
USER_AGENT = "repo-radar/0.1 (github graphql)"
DEFAULT_BATCH_SIZE = 50
MIN_REMAINING_POINTS = 50
MAX_RETRIES = 5


REPO_FIELDS = """
  databaseId
  nameWithOwner
  owner { login }
  name
  description
  primaryLanguage { name }
  stargazerCount
  forkCount
  pushedAt
  updatedAt
  createdAt
"""


def chunked(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _parse_github_dt(value: Optional[str]) -> Optional[datetime]:
    """ISO ``2024-01-15T10:30:00Z`` → naive UTC datetime (как в П4)."""
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class GitHubGraphQLClient:
    """POST /graphql с Bearer-токеном, retry и паузой по rateLimit.
    Args:
        token: ``GITHUB_TOKEN``. Без него GraphQL почти сразу 401.
        batch_size: Сколько ``repository(...)`` aliases в одном query.
        timeout: HTTP timeout секунд.
    """

    def __init__(
        self,
        token: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        timeout: float = 30.0,
    ):
        if not token:
            raise ValueError("GITHUB_TOKEN пустой — GraphQL не запустится")
        self.token = token
        self.batch_size = batch_size
        self.http = httpx.Client(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": USER_AGENT,
                "Content-Type": "application/json",
            },
        )

    def close(self) -> None:
        self.http.close()

    def fetch_repos(self, repos: list[dict]) -> list[dict]:
        """Тянет все репо из seed. ``repos`` — элементы tracked_repos.yml.
        Returns:
            Плоские dict под parquet / ``silver_repos``.
        """
        rows: list[dict] = []
        batches = list(chunked(repos, self.batch_size))
        total = len(batches)
        for i, batch in enumerate(batches, start=1):
            print(f"   GraphQL батч {i}/{total} ({len(batch)} репо)...")
            rows.extend(self._fetch_batch(batch))
        return rows

    def _build_query(self, batch: list[dict]) -> str:
        aliases = []
        for idx, repo in enumerate(batch):
            owner = json.dumps(repo["owner"])
            name = json.dumps(repo["name"])
            aliases.append(
                f"  r{idx}: repository(owner: {owner}, name: {name}) {{"
                f"{REPO_FIELDS}"
                f"  }}"
            )
        body = "\n".join(aliases)
        return (
            "query {\n"
            "  rateLimit { cost remaining resetAt limit }\n"
            f"{body}\n"
            "}"
        )

    def _post(self, query: str) -> dict:
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.http.post(
                    GITHUB_GRAPHQL_URL,
                    json={"query": query},
                )
            except httpx.HTTPError as e:
                last_exc = e
                sleep_s = min(2 ** attempt, 60)
                print(f"   ⚠️  сеть: {e}; retry {attempt}/{MAX_RETRIES} через {sleep_s}s")
                time.sleep(sleep_s)
                continue

            if resp.status_code in (403, 429, 502, 503):
                retry_after = resp.headers.get("Retry-After")
                sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 60)
                print(
                    f"   ⚠️  HTTP {resp.status_code}; "
                    f"retry {attempt}/{MAX_RETRIES} через {sleep_s}s"
                )
                time.sleep(sleep_s)
                continue

            resp.raise_for_status()
            return resp.json()

        raise RuntimeError(f"GraphQL не ответил после {MAX_RETRIES} попыток: {last_exc}")

    def _maybe_wait_rate_limit(self, rate: Optional[dict]) -> None:
        if not rate:
            return
        remaining = int(rate.get("remaining") or 0)
        reset_at = rate.get("resetAt")
        if remaining >= MIN_REMAINING_POINTS or not reset_at:
            return
        reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        wait = max(0, (reset_dt - now).total_seconds()) + 1
        print(f"   ⏳ rateLimit remaining={remaining}, ждём {wait:.0f}s до {reset_at}")
        time.sleep(wait)

    def _fetch_batch(self, batch: list[dict]) -> list[dict]:
        payload = self._post(self._build_query(batch))
        data = payload.get("data") or {}
        self._maybe_wait_rate_limit(data.get("rateLimit"))

        errors = payload.get("errors") or []
        for err in errors:
            path = err.get("path") or []
            print(f"   ⚠️  GraphQL: {err.get('message')} path={path}")

        rows = []
        for idx, seed in enumerate(batch):
            node = data.get(f"r{idx}")
            if not node:
                print(f"   ⚠️  нет данных: {seed['owner']}/{seed['name']}")
                continue
            row = self._transform(node, seed)
            if row is not None:
                rows.append(row)
        return rows

    @staticmethod
    def _transform(node: dict, seed: dict) -> Optional[dict]:
        created_at = _parse_github_dt(node.get("createdAt"))
        updated_at = _parse_github_dt(node.get("updatedAt")) or created_at

        repo_name = node.get("nameWithOwner") or f"{seed['owner']}/{seed['name']}"
        
        if updated_at is None:
            print(f"   ⚠️  skip {repo_name}: нет updatedAt/createdAt")
            return None

        lang_obj = node.get("primaryLanguage") or {}
        language = lang_obj.get("name") or seed.get("language")
        
        if language:
            language = str(language).lower()
            
        return {
            "repo_id": node.get("databaseId") or 0,
            "repo_name": repo_name,
            "owner": (node.get("owner") or {}).get("login") or seed["owner"],
            "name": node.get("name") or seed["name"],
            "description": node.get("description"),
            "language": language,
            "stars": node.get("stargazerCount") or 0,
            "forks": node.get("forkCount") or 0,
            "pushed_at": _parse_github_dt(node.get("pushedAt")),
            "updated_at": updated_at,
            "created_at": created_at,
            "ecosystem": seed.get("ecosystem") or "unknown",
            "package": seed.get("package") or seed["name"],
        }


