"""Парсинг дат из внешних API в naive UTC datetime.

В bronze/silver даты хранятся без tzinfo (как в П4/П5). Внешние API
(GitHub GraphQL, OSV) отдают ISO-8601 / RFC3339 со суффиксом ``Z`` или
offset'ом — этот модуль приводит их к единому виду.
"""

from __future__ import annotations

from datetime import UTC, datetime


def parse_iso_utc_naive(value: str | None) -> datetime | None:
    """ISO/RFC3339 строка → naive UTC datetime, либо ``None``.

    Поддерживает:
      * ``2024-01-15T10:30:00Z``
      * ``2024-01-15T10:30:00+00:00``
      * строки без tz (остаются как есть)

    Args:
        value: Дата из API. Пустая строка / ``None`` → ``None``.

    Returns:
        ``datetime`` без ``tzinfo`` в UTC, либо ``None``.

    Notes:
        Не ловит ``ValueError`` — битая строка должна падать вверх
        (как сейчас в GraphQL/OSV клиентах). Если понадобится soft-fail,
        оберни вызов в try/except на call-site.
    """
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


