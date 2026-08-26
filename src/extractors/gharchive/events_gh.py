import gzip
import json
from pathlib import Path
from typing import Iterator, Optional

import yaml

ALLOWED_EVENT_TYPES = {
    "WatchEvent",
    "ForkEvent",
    "ReleaseEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "PushEvent",
}


def load_tracked_repos(tracked_repos_path: Path) -> set[str]:
    with open(tracked_repos_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    repos = set()
    for repo in data.get("repositories", []):
        repos.add(f"{repo['owner']}/{repo['name']}")
    return repos


def read_events(archive_path: Path) -> Iterator[dict]:
    with gzip.open(archive_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"   ⚠️  Ошибка парсинга JSON: {e}")


def filter_events(
    events: Iterator[dict],
    tracked_repos: set[str],
    allowed_types: Optional[set[str]] = None,
) -> Iterator[dict]:
    if allowed_types is None:
        allowed_types = ALLOWED_EVENT_TYPES

    for event in events:
        if event.get("type") not in allowed_types:
            continue
        repo_name = event.get("repo", {}).get("name")
        if repo_name not in tracked_repos:
            continue
        yield event


def transform_event(event: dict) -> dict:
    transformed = {
        "event_id": event.get("id"),
        "event_type": event.get("type"),
        "event_time": event.get("created_at"),
        "actor_id": event.get("actor", {}).get("id"),
        "actor_login": event.get("actor", {}).get("login"),
        "repo_id": event.get("repo", {}).get("id"),
        "repo_name": event.get("repo", {}).get("name"),
        "public": event.get("public", True),
    }

    payload = event.get("payload", {})
    event_type = event.get("type")

    if event_type == "PullRequestEvent":
        pr = payload.get("pull_request", {})
        transformed.update({
            "pr_action": payload.get("action"),
            "pr_number": payload.get("number"),
            "pr_merged": pr.get("merged"),
        })
    elif event_type == "IssuesEvent":
        transformed.update({
            "issue_action": payload.get("action"),
            "issue_number": payload.get("issue", {}).get("number"),
        })
    elif event_type == "PushEvent":
        transformed.update({
            "push_size": payload.get("size"),
            "push_ref": payload.get("ref"),
        })
    elif event_type == "ReleaseEvent":
        release = payload.get("release", {})
        transformed.update({
            "release_tag": release.get("tag_name"),
            "release_name": release.get("name"),
        })

    return transformed


