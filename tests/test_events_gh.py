from pathlib import Path

from extractors.gharchive.events_gh import (
    ALLOWED_EVENT_TYPES,
    load_tracked_repos,
    transform_event,
)


def test_allowed_event_types_not_empty():
    assert "WatchEvent" in ALLOWED_EVENT_TYPES


def test_transform_event_flattens_watch():
    raw = {
        "id": "1",
        "type": "WatchEvent",
        "created_at": "2026-09-10T00:00:00Z",
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 2, "name": "org/repo"},
        "public": True,
        "payload": {},
    }
    out = transform_event(raw)
    assert out["event_id"] == "1"
    assert out["repo_name"] == "org/repo"
    assert out["pr_merged"] is None


def test_load_tracked_repos(tmp_path: Path):
    p = tmp_path / "repos.yml"
    p.write_text(
        "repositories:\n  - owner: pallets\n    name: flask\n",
        encoding="utf-8",
    )
    assert load_tracked_repos(p) == {"pallets/flask"}


