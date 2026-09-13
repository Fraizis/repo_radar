"""Загрузка источников changelog из ``config/changelog_sources.yml``.

Формат YAML::

    sources:
      - name: flask
        url: https://...
        parser: html
        selectors:
          version: "h2"
          date: null
"""

from __future__ import annotations

from pathlib import Path

import yaml

from config.paths import CHANGELOG_SOURCES_YAML


def load_changelog_sources(config_path: Path | None = None) -> list[dict]:
    """Читает YAML → список источников (name / url / selectors / ...).

    Args:
        config_path: Путь к YAML. По умолчанию ``CHANGELOG_SOURCES_YAML``.

    Returns:
        Список dict. Пустой файл / нет ключа ``sources`` → ``[]``.
    """
    path = config_path or CHANGELOG_SOURCES_YAML
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return list(data.get("sources") or [])

