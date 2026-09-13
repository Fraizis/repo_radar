"""Checkpoint скрейпа changelog'ов: какой URL уже тянули недавно.

Файл: ``{checkpoint_dir}/changelogs.json``
  ``{url: {scraped_at: iso, versions: int}}``
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


class ChangelogCheckpoint:
    def __init__(self, checkpoint_dir: Path):
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.path = checkpoint_dir / "changelogs.json"

    def load(self) -> dict:
        """Читает чекпоинт; пустой dict, если файла нет."""
        if not self.path.exists():
            return {}
        with open(self.path, encoding="utf-8") as f:
            return json.load(f) or {}

    def update(self, checkpoint: dict, url: str, versions: int) -> None:
        """Обновляет запись по URL и сразу пишет на диск."""
        checkpoint[url] = {
            "scraped_at": datetime.now(UTC).isoformat(),
            "versions": versions,
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)


