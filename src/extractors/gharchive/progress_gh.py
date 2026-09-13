"""
Прогресс скачивания GH Archive без сторонних библиотек.

``ProgressCallback`` — любой callable ``(downloaded: int, total: Optional[int])``.
``ConsoleDownloadProgress`` рисует бар в TTY (``\\r``) и редкие строки, если stdout
не терминал. ``close()`` печатает перевод строки после последнего ``\\r``.
"""

import sys
import time
from collections.abc import Callable

from utils.formatting import format_duration, format_size

"""Сигнатура колбэка прогресса: (скачано байт, ожидаемый размер или None)."""
ProgressCallback = Callable[[int, int | None], None]


class ConsoleDownloadProgress:
    """
    Прогресс-бар скачивания без внешних зависимостей.
    Использование как колбэка: ``progress(downloaded_bytes, total_bytes)``.
    По завершении вызвать ``close()`` — допишет перевод строки в TTY.
    Args:
        filename: Имя файла в логах (сейчас не печатается в строке бара,
            но хранится для идентификации).
        min_interval: Минимальный интервал перерисовки, сек. Для не-TTY
            принудительно не меньше 5 сек, чтобы не спамить логи.
        stream: Куда писать (по умолчанию ``sys.stdout``).
    """

    BAR_WIDTH = 30

    def __init__(self, filename: str, min_interval: float = 0.2, stream=None):
        self.filename = filename
        self.stream = stream if stream is not None else sys.stdout
        self.is_tty = hasattr(self.stream, "isatty") and self.stream.isatty()
        self.min_interval = min_interval if self.is_tty else max(min_interval, 5.0)
        self._started_at = time.monotonic()
        self._last_render = 0.0
        self._line_width = 0
        self._dirty = False

    def __call__(self, downloaded: int, total: int | None) -> None:
        """Обновить бар. Троттлится по ``min_interval``, кроме финального тика.
        Args:
            downloaded: Уже скачанные байты.
            total: Ожидаемый размер из ``Content-Length``, либо ``None``
                (тогда бар без процента и ETA).
        """
        now = time.monotonic()
        is_final = total is not None and downloaded >= total
        if not is_final and (now - self._last_render) < self.min_interval:
            return
        self._last_render = now

        elapsed = max(now - self._started_at, 1e-6)
        speed = downloaded / elapsed

        if total:
            fraction = min(downloaded / total, 1.0)
            filled = int(self.BAR_WIDTH * fraction)
            bar = "█" * filled + "░" * (self.BAR_WIDTH - filled)
            eta = (total - downloaded) / speed if speed > 0 else float("inf")
            line = (
                f"   [{bar}] {fraction * 100:5.1f}%  "
                f"{format_size(downloaded)} / {format_size(total)}  "
                f"{format_size(speed)}/s  ETA {format_duration(eta)}"
            )
        else:
            line = (
                f"   ⬇️   {format_size(downloaded)}  "
                f"{format_size(speed)}/s  {format_duration(elapsed)}"
            )

        prefix = "\r" if self.is_tty else ""
        suffix = "" if self.is_tty else "\n"
        self.stream.write(prefix + line.ljust(self._line_width) + suffix)
        self.stream.flush()
        self._line_width = max(self._line_width, len(line))
        self._dirty = self.is_tty

    def close(self) -> None:
        """Если последняя отрисовка была через ``\\r``, печатает ``\\n``.
        Идемпотентен: повторный вызов ничего не делает.
        """
        if self._dirty:
            self.stream.write("\n")
            self.stream.flush()
            self._dirty = False


