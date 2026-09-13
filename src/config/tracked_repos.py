"""Загрузка seed-репозиториев из ``config/tracked_repos.yml``.

Два представления одних и тех же данных:
  * ``load_repositories`` → ``list[dict]`` (GraphQL, OSV — нужны поля)
  * ``as_full_names``     → ``set[str]`` вида ``owner/name`` (фильтр GH Archive)

Формат YAML::

    repositories:
      - owner: pallets
        name: flask
        ecosystem: pypi
        package: flask
        language: python
"""

from __future__ import annotations

from pathlib import Path

import yaml

from config.paths import TRACKED_REPOS_YAML


def load_repositories(path: Path | None = None) -> list[dict]:
    """Читает YAML → список seed-dict по ключу ``repositories``.

    Args:
        path: Путь к YAML. По умолчанию ``TRACKED_REPOS_YAML``.

    Returns:
        Список элементов. Пустой файл / нет ключа → ``[]``.
    """
    yaml_path = path or TRACKED_REPOS_YAML
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return list(data.get("repositories") or [])


def as_full_names(repos: list[dict]) -> set[str]:
    """Чистая трансформация: seed-dict → множество ``owner/name``.

    Без I/O — удобно, если список уже загружен и нужен ещё и set
    (не читать YAML дважды).

    Args:
        repos: Результат ``load_repositories``.

    Returns:
        Например ``{"pallets/flask", "gin-gonic/gin"}``.

    Raises:
        KeyError: Если у элемента нет ``owner`` или ``name``.
    """
    return {f"{repo['owner']}/{repo['name']}" for repo in repos}


def load_tracked_repos(path: Path | None = None) -> set[str]:
    """Удобная обёртка для GH Archive: YAML → ``set[owner/name]``.

    Эквивалент ``as_full_names(load_repositories(path))``.
    """
    return as_full_names(load_repositories(path))

