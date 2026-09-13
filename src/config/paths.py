"""Корневые пути проекта.

Единая точка вместо повторного ``Path(__file__).parent.parent...``
в assets. YAML-конфиги лежат в ``<repo>/config/``, а не в ``src/config/``.
"""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

TRACKED_REPOS_YAML: Path = PROJECT_ROOT / "config" / "tracked_repos.yml"
CHANGELOG_SOURCES_YAML: Path = PROJECT_ROOT / "config" / "changelog_sources.yml"

CHECKPOINT_DIR: Path = PROJECT_ROOT / "data" / "checkpoints"
DOWNLOAD_DIR: Path = PROJECT_ROOT / "data" / "downloads"

OSV_CACHE_DIR: Path = PROJECT_ROOT / "data" / "cache" / "osv"

TRANSFORM_DIR: Path = PROJECT_ROOT / "transform"

