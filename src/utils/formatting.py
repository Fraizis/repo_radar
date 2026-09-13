"""Человекочитаемое форматирование размеров и длительностей."""

from __future__ import annotations


def format_size(num_bytes: float) -> str:
    """Байты → строка вроде ``1.5 KB`` / ``12.0 MB``."""
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(value) < 1024 or unit == "GB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def format_duration(seconds: float) -> str:
    """Секунды → ``MM:SS`` или ``HH:MM:SS``. NaN/inf → ``--:--``."""
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        return "--:--"
    total = int(max(seconds, 0))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"

