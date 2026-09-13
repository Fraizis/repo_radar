"""Парсинг version/date из текста changelog-заголовков."""

from __future__ import annotations

import re
from datetime import UTC, datetime

_VERSION_RE = re.compile(
    r"\d+\.\d+(?:\.\d+)?(?:[.\-]?(?:a|b|rc|alpha|beta|dev|post)\d*)?",
    re.IGNORECASE,
)

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d %B %Y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%d %b %Y",
)


def clean_version(text: str | None) -> str | None:
    """Из 'Version 3.1.0 (2024-...)' → '3.1.0'."""
    if not text:
        return None
    m = _VERSION_RE.search(text)
    return m.group(0) if m else None


def parse_changelog_date(text: str | None) -> datetime | None:
    """Несколько форматов дат → naive UTC datetime."""
    if not text:
        return None
    text = text.strip()
    iso = re.search(r"\d{4}-\d{2}-\d{2}", text)
    candidates = ([iso.group(0)] if iso else []) + [text]
    for cand in candidates:
        for fmt in _DATE_FORMATS:
            try:
                dt = datetime.strptime(cand, fmt)
                return dt.replace(tzinfo=UTC).replace(tzinfo=None)
            except ValueError:
                continue
    return None

