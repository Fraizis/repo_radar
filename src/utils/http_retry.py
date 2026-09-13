"""HTTP retry с экспоненциальным backoff и уважением Retry-After.

Используется httpx-клиентами (GitHub GraphQL, OSV). Playwright-навигация
не ходит через httpx — для неё достаточно ``backoff_seconds``.
"""

from __future__ import annotations

import time

import httpx

DEFAULT_RETRY_STATUSES: tuple[int, ...] = (429, 500, 502, 503)

GRAPHQL_RETRY_STATUSES: tuple[int, ...] = (403, 429, 502, 503)


def backoff_seconds(
    attempt: int,
    retry_after: str | None = None,
    *,
    cap: float = 60.0,
) -> float:
    """Сколько спать перед следующей попыткой.

    Порядок приоритета:
      1. Заголовок ``Retry-After`` (секунды, целое) — если валидный.
      2. ``min(2 ** attempt, cap)`` — экспоненциальный backoff.

    Args:
        attempt: Номер попытки, начиная с 1.
        retry_after: Сырое значение заголовка ``Retry-After`` или ``None``.
        cap: Верхняя граница backoff в секундах.

    Returns:
        Секунды ожидания (``float``).
    """
    if retry_after is not None and retry_after.isdigit():
        return float(int(retry_after))
    return float(min(2**attempt, cap))


def request_json_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    json_body: dict | None = None,
    max_retries: int = 5,
    retry_statuses: tuple[int, ...] = DEFAULT_RETRY_STATUSES,
    label: str = "HTTP",
    cap: float = 60.0,
) -> dict:
    """``client.request`` + retry на сеть / выбранные HTTP-статусы → ``resp.json()``.

    Поведение совпадает с прежними циклами в GraphQL ``_post`` и OSV ``_request``:
      * сетевые ``httpx.HTTPError`` → sleep + retry;
      * статус из ``retry_statuses`` → sleep (с ``Retry-After``) + retry;
      * прочие статусы → ``raise_for_status()``;
      * исчерпаны попытки → ``RuntimeError``.

    Args:
        client: Уже созданный ``httpx.Client`` (с заголовками/таймаутом).
        method: ``GET`` / ``POST`` / ...
        url: Полный URL.
        json_body: Тело JSON (для POST) или ``None``.
        max_retries: Максимум попыток (включительно).
        retry_statuses: HTTP-коды, на которых ретраим.
        label: Имя сервиса в логах / тексте ошибки (``GraphQL``, ``OSV``).
        cap: Потолок backoff.

    Returns:
        Распарсенный JSON (``dict``).

    Raises:
        RuntimeError: Все попытки исчерпаны.
        httpx.HTTPStatusError: Неретраимый HTTP-статус.
    """
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.request(method, url, json=json_body)
        except httpx.HTTPError as e:
            last_exc = e
            sleep_s = backoff_seconds(attempt, cap=cap)
            print(f"   ⚠️  сеть: {e}; retry {attempt}/{max_retries} через {sleep_s}s")
            time.sleep(sleep_s)
            continue

        if resp.status_code in retry_statuses:
            sleep_s = backoff_seconds(
                attempt,
                resp.headers.get("Retry-After"),
                cap=cap,
            )
            print(
                f"   ⚠️  HTTP {resp.status_code}; "
                f"retry {attempt}/{max_retries} через {sleep_s}s"
            )
            time.sleep(sleep_s)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"{label} не ответил после {max_retries} попыток: {last_exc}")

