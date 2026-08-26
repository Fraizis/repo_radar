import gzip
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

from extractors.gharchive.progress_gh import (
    ConsoleDownloadProgress,
    ProgressCallback,
    format_size,
)

BASE_URL = "https://data.gharchive.org"
USER_AGENT = "repo-radar/0.1 (gharchive downloader)"


def archive_filename(dt: datetime) -> str:
    return f"{dt:%Y-%m-%d}-{dt.hour}.json.gz"


class GitHubArchiveDownloader:
    def __init__(self, download_dir: Path):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def is_valid_archive(archive_path: Path) -> bool:
        try:
            with gzip.open(archive_path, "rb") as archive:
                while archive.read(1024 * 1024):
                    pass
            return True
        except (EOFError, gzip.BadGzipFile, OSError):
            return False

    @staticmethod
    def parse_content_length(raw_value: Optional[str]) -> Optional[int]:
        if not raw_value:
            return None
        try:
            size = int(raw_value)
        except (TypeError, ValueError):
            return None
        return size if size > 0 else None

    def download_archive(
        self,
        dt: datetime,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Path:
        filename = archive_filename(dt)
        url = f"{BASE_URL}/{filename}"

        output_path = self.download_dir / filename
        temporary_path = Path(str(output_path) + ".part")

        if output_path.exists():
            if self.is_valid_archive(output_path):
                print(f"   ⏭️    Файл уже существует: {filename}")
                return output_path

            print(f"   ⚠️    Архив повреждён или скачан не полностью: {filename}")
            print("   🔄 Удаляем его и скачиваем заново")
            output_path.unlink()

        temporary_path.unlink(missing_ok=True)
        print(f"   ⬇️    Скачиваем: {url}")

        progress = progress_callback or ConsoleDownloadProgress(filename)

        try:
            with httpx.stream(
                "GET",
                url,
                timeout=300,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            ) as response:
                response.raise_for_status()

                total_size = self.parse_content_length(
                    response.headers.get("Content-Length")
                )
                progress(0, total_size)

                with open(temporary_path, "wb") as f:
                    for chunk in response.iter_raw(chunk_size=256 * 1024):
                        f.write(chunk)
                        progress(response.num_bytes_downloaded, total_size)

                total_bytes = response.num_bytes_downloaded

            if total_size is not None and total_bytes != total_size:
                raise EOFError(
                    f"Оборванная загрузка {url}: "
                    f"получено {total_bytes} из {total_size} байт"
                )

            if not self.is_valid_archive(temporary_path):
                raise EOFError(f"Скачан неполный или повреждённый архив: {url}")

            temporary_path.replace(output_path)
            print(f"   ✓ Скачано: {format_size(total_bytes)}")
            return output_path

        except httpx.HTTPStatusError as e:
            temporary_path.unlink(missing_ok=True)
            if e.response.status_code == 404:
                raise FileNotFoundError(f"Архив не найден: {url}") from e
            raise

        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise

        finally:
            close = getattr(progress, "close", None)
            if callable(close):
                close()


