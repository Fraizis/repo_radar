"""
Playwright-скрейпер changelog'ов.
Читает источники из config/changelog_sources.yml, рендерит страницу в headless
Chromium и вытаскивает по CSS-селекторам: version, date, heading.

Уважает robots.txt (urllib.robotparser), делает экспоненциальный backoff на
навигации. Один браузер на весь прогон (context manager).
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib import robotparser
from urllib.parse import urlsplit

import httpx
import yaml
from playwright.sync_api import Error as PWError
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

USER_AGENT = "repo-radar/0.1 (changelog scraper; +https://github.com)"
MAX_RETRIES = 4
NAV_TIMEOUT_MS = 30_000

_VERSION_RE = re.compile(
    r"\d+\.\d+(?:\.\d+)?(?:[.\-]?(?:a|b|rc|alpha|beta|dev|post)\d*)?",
    re.IGNORECASE,
)

# форматы дат, которые встречаются в changelog'ах
_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d %B %Y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%d %b %Y",
)


def load_changelog_sources(config_path: Path) -> list[dict]:
    """Читает config/changelog_sources.yml → список источников (name/url/selectors)."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return list(data.get("sources") or [])


def _clean_version(text: Optional[str]) -> Optional[str]:
    """Из заголовка 'Version 3.1.0 (2024-...)' достаёт '3.1.0'."""
    if not text:
        return None
    m = _VERSION_RE.search(text)
    return m.group(0) if m else None


def _parse_date(text: Optional[str]) -> Optional[datetime]:
    """Пытается распарсить дату несколькими форматами → naive UTC (как в П4/П5)."""
    if not text:
        return None
    text = text.strip()
    iso = re.search(r"\d{4}-\d{2}-\d{2}", text)
    candidates = ([iso.group(0)] if iso else []) + [text]
    for cand in candidates:
        for fmt in _DATE_FORMATS:
            try:
                dt = datetime.strptime(cand, fmt)
                return dt.replace(tzinfo=timezone.utc).replace(tzinfo=None)
            except ValueError:
                continue
    return None


class ChangelogScraper:
    """Headless Chromium + парсинг по селекторам. Использовать как context manager."""

    def __init__(self, timeout_ms: int = NAV_TIMEOUT_MS):
        self.timeout_ms = timeout_ms
        self._pw = None
        self._browser = None
        self._robots_cache: dict[str, Optional[robotparser.RobotFileParser]] = {}

    def __enter__(self) -> "ChangelogScraper":
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        return self

    def __exit__(self, *exc) -> None:
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()

    def _robots_allows(self, url: str) -> bool:
        """True, если robots.txt разрешает наш UA тянуть URL.
        robots.txt качаем сами (httpx + наш UA), т.к. urllib.robotparser
        под дефолтным Python-UA ловит 403 у Cloudflare-сайтов и фейлит closed.
        Нет robots / 4xx / ошибка сети → fail-open (разрешено)."""
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"
        if base not in self._robots_cache:
            rp = robotparser.RobotFileParser()
            try:
                resp = httpx.get(
                    f"{base}/robots.txt",
                    headers={"User-Agent": USER_AGENT},
                    timeout=15,
                    follow_redirects=True,
                )
                if resp.status_code >= 400:
                    rp = None
                else:
                    rp.parse(resp.text.splitlines())
            except Exception as e:  # noqa: BLE001
                print(f"   ⚠️  robots.txt недоступен ({base}): {e}; считаем разрешённым")
                rp = None
            self._robots_cache[base] = rp

        rp = self._robots_cache[base]
        return True if rp is None else rp.can_fetch(USER_AGENT, url)

    def _goto_with_retry(self, page, url: str) -> bool:
        """page.goto с экспоненциальным backoff. False, если не удалось."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                return True
            except (PWTimeout, PWError) as e:
                sleep_s = min(2 ** attempt, 30)
                print(f"   ⚠️  goto {url}: {e}; retry {attempt}/{MAX_RETRIES} через {sleep_s}s")
                time.sleep(sleep_s)
        return False

    def scrape(self, source: dict) -> list[dict]:
        """Один источник → список строк под silver_changelogs.
        Args:
            source: элемент sources из changelog_sources.yml.
        """
        name = source.get("name") or "unknown"
        url = source["url"]
        selectors = source.get("selectors") or {}
        version_sel = selectors.get("version") or "h2"
        date_sel = selectors.get("date")  # может быть null

        if not self._robots_allows(url):
            print(f"   🚫 robots.txt запрещает {url} — пропуск '{name}'")
            return []

        page = self._browser.new_page(user_agent=USER_AGENT)
        try:
            if not self._goto_with_retry(page, url):
                print(f"   ✗ не удалось открыть '{name}' ({url})")
                return []

            try:
                page.wait_for_selector(version_sel, timeout=self.timeout_ms)
            except PWTimeout:
                print(f"   ⚠️  '{name}': селектор '{version_sel}' не появился")
                
            scraped_at = datetime.now(timezone.utc).replace(tzinfo=None)
            rows = self._extract(page, name, url, version_sel, date_sel, scraped_at)
            print(f"   ✓ '{name}': {len(rows)} версий")
            return rows
        finally:
            page.close()

    @staticmethod
    def _extract(page, name, url, version_sel, date_sel, scraped_at) -> list[dict]:
        """Достаёт version / date / heading из отрендеренного DOM.
        Каждый найденный version-заголовок → одна строка. Даты сопоставляются
        по индексу (best-effort), если задан date_sel."""
        version_els = page.query_selector_all(version_sel)
        date_texts: list[str] = []
        if date_sel:
            date_texts = [el.inner_text() for el in page.query_selector_all(date_sel)]

        rows: list[dict] = []
        for idx, el in enumerate(version_els):
            heading = (el.inner_text() or "").strip()
            version = _clean_version(heading)
            if not version:
                continue

            raw_date = date_texts[idx] if idx < len(date_texts) else heading
            rows.append({
                "source": name,
                "url": url,
                "version": version,
                "heading": heading[:512],
                "release_date": _parse_date(raw_date),
                "scraped_at": scraped_at,
            })
        return rows

