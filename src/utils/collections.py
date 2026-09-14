"""Утилиты для работы с последовательностями.

Общие хелперы без доменной логики. Сейчас используется клиентами
GitHub GraphQL и OSV для нарезки seed/querybatch на батчи.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence


def chunked[T](items: Sequence[T], size: int) -> Iterator[list[T]]:
    """Режет последовательность на куски фиксированного размера.

    Последний кусок может быть короче ``size``. Пустой ``items`` → пустой
    итератор (ни одного yield).

    Args:
        items: Список / tuple / любая Sequence.
        size: Размер куска. Должен быть ``>= 1``.

    Yields:
        Списки длины ``size`` (кроме последнего).

    Raises:
        ValueError: Если ``size < 1``.

    Examples:
        >>> list(chunked([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    if size < 1:
        raise ValueError(f"size must be >= 1, got {size}")
    for i in range(0, len(items), size):
        yield list(items[i : i + size])

