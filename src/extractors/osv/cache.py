"""Дисковый кэш деталей OSV vuln (`GET /v1/vulns/{id}`).

Ключ инвалидации — поле ``modified`` из querybatch: если в файле другое
значение, считаем кэш протухшим и тянем заново.
"""

from __future__ import annotations

import json
from pathlib import Path


def cache_path(cache_dir: Path, vid: str) -> Path:
    """Путь к JSON-файлу уязвимости (``/`` в id → ``_``)."""
    return cache_dir / f"{vid.replace('/', '_')}.json"


def cache_get(
    cache_dir: Path | None,
    vid: str,
    modified: str | None,
) -> dict | None:
    """Вернуть детали из кэша, если файл есть и ``modified`` совпадает.

    Args:
        cache_dir: Каталог кэша; ``None`` — кэш выключен.
        vid: OSV id (например ``GHSA-...``).
        modified: Значение ``modified`` из querybatch; ``None`` — не сверять.

    Returns:
        Dict деталей или ``None``.
    """
    if cache_dir is None:
        return None
    path = cache_path(cache_dir, vid)
    if not path.exists():
        return None
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if modified is not None and blob.get("modified") != modified:
        return None
    return blob


def cache_put(cache_dir: Path | None, vid: str, data: dict) -> None:
    """Пишет JSON на диск. Ошибки I/O глотаются (кэш best-effort)."""
    if cache_dir is None:
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        cache_path(cache_dir, vid).write_text(
            json.dumps(data, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError:
        pass

