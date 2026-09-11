"""
OSV.dev клиент: уязвимости по (ecosystem, package) из seed.
Два шага:
  * POST /v1/querybatch — по списку пакетов, отдаёт только id уязвимостей
  * GET  /v1/vulns/{id} — полный объект (severity, published, aliases)
API публичный, токен не нужен. Вежливый rate limit через backoff.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import httpx

OSV_BASE_URL = "https://api.osv.dev"
USER_AGENT = "repo-radar/0.1 (osv)"
DEFAULT_BATCH_SIZE = 100
MAX_RETRIES = 5

ECOSYSTEM_MAP = {
    "pypi": "PyPI",
    "go": "Go",
    "cargo": "crates.io",
    "npm": "npm",
}


def chunked(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _parse_osv_dt(value: str | None) -> datetime | None:
    """RFC3339 '2024-01-15T10:30:00Z' → naive UTC (как в П4/П5)."""
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def _cache_path(cache_dir: Path, vid: str) -> Path:
    return cache_dir / f"{vid.replace('/', '_')}.json"


def _cache_get(cache_dir: Path | None, vid: str, modified: str | None) -> dict | None:
    """Вернуть детали из кэша, если файл есть и modified совпадает; иначе None."""
    if cache_dir is None:
        return None
    p = _cache_path(cache_dir, vid)
    if not p.exists():
        return None
    try:
        blob = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if modified is not None and blob.get("modified") != modified:
        return None
    return blob


def _cache_put(cache_dir: Path | None, vid: str, data: dict) -> None:
    if cache_dir is None:
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        _cache_path(cache_dir, vid).write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8"
        )
    except OSError:
        pass


class OSVClient:
    """Клиент OSV.dev с ретраями. Без токена (публичный API)."""

    def __init__(self, batch_size: int = DEFAULT_BATCH_SIZE, timeout: float = 30.0):
        self.batch_size = batch_size
        self.http = httpx.Client(
            timeout=timeout,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

    def close(self) -> None:
        self.http.close()

    def _request(self, method: str, url: str, json_body: dict | None = None) -> dict:
        last_exc: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.http.request(method, url, json=json_body)
            except httpx.HTTPError as e:
                last_exc = e
                sleep_s = min(2**attempt, 60)
                print(f"   ⚠️  сеть: {e}; retry {attempt}/{MAX_RETRIES} через {sleep_s}s")
                time.sleep(sleep_s)
                continue

            if resp.status_code in (429, 500, 502, 503):
                retry_after = resp.headers.get("Retry-After")
                sleep_s = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else min(2**attempt, 60)
                )
                print(
                    f"   ⚠️  HTTP {resp.status_code}; retry {attempt}/{MAX_RETRIES} через {sleep_s}s"
                )
                time.sleep(sleep_s)
                continue

            resp.raise_for_status()
            return resp.json()

        raise RuntimeError(f"OSV не ответил после {MAX_RETRIES} попыток: {last_exc}")

    def fetch_advisories(
        self,
        repos: list[dict],
        max_workers: int = 8,
        cache_dir: Path | None = Path("data/cache/osv"),
    ) -> list[dict]:
        """seed → плоские строки под silver_advisories.
        Args:
            repos: элементы tracked_repos.yml (owner/name/ecosystem/package).
            max_workers: параллелизм дотяжки деталей (I/O-bound).
            cache_dir: каталог кэша деталей; None — кэш выключен.
        """
        queryable: list[dict] = []
        for r in repos:
            eco = ECOSYSTEM_MAP.get((r.get("ecosystem") or "").lower())
            pkg = r.get("package")
            if not eco or not pkg:
                continue
            queryable.append(
                {
                    "repo_name": f"{r['owner']}/{r['name']}",
                    "ecosystem": eco,
                    "package": pkg,
                }
            )

        pairs: list[tuple[dict, str]] = []
        vuln_ids: set[str] = set()
        vuln_modified: dict[str, str] = {}
        batches = list(chunked(queryable, self.batch_size))
        for i, batch in enumerate(batches, start=1):
            print(f"   OSV querybatch {i}/{len(batches)} ({len(batch)} пакетов)...")
            body = {
                "queries": [
                    {"package": {"name": q["package"], "ecosystem": q["ecosystem"]}} for q in batch
                ]
            }
            payload = self._request("POST", f"{OSV_BASE_URL}/v1/querybatch", body)
            for q, res in zip(batch, payload.get("results") or []):
                for v in res.get("vulns") or []:
                    vid = v.get("id")
                    if vid:
                        pairs.append((q, vid))
                        vuln_ids.add(vid)
                        m = v.get("modified")
                        if m:
                            vuln_modified[vid] = m

        print(f"   ✓ Найдено {len(vuln_ids)} уникальных уязвимостей")

        details: dict[str, dict] = {}
        unique_ids = sorted(vuln_ids)
        total = len(unique_ids)

        to_fetch: list[str] = []
        for vid in unique_ids:
            cached = _cache_get(cache_dir, vid, vuln_modified.get(vid))
            if cached is not None:
                details[vid] = cached
            else:
                to_fetch.append(vid)

        print(f"   кэш: {total - len(to_fetch)}/{total} готово, тянем {len(to_fetch)}")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for n, (vid, data) in enumerate(pool.map(self._fetch_vuln, to_fetch), start=1):
                details[vid] = data
                _cache_put(cache_dir, vid, data)
                if n % 100 == 0:
                    print(f"   детали {n}/{len(to_fetch)}...")

        rows = [self._transform(seed, details.get(vid) or {}) for seed, vid in pairs]
        return rows

    def _fetch_vuln(self, vid: str) -> tuple[str, dict]:
        """GET одной уязвимости — вызывается из пула потоков."""
        return vid, self._request("GET", f"{OSV_BASE_URL}/v1/vulns/{vid}")

    @staticmethod
    def _transform(seed: dict, vuln: dict) -> dict:
        aliases = vuln.get("aliases") or []
        cve_id = next((a for a in aliases if a.startswith("CVE-")), None)

        db = vuln.get("database_specific") or {}
        severity_label = (db.get("severity") or "").upper() or "UNKNOWN"

        sev_list = vuln.get("severity") or []
        cvss_vector = sev_list[0].get("score") if sev_list else None

        return {
            "vuln_id": vuln.get("id") or "",
            "cve_id": cve_id,
            "aliases": ",".join(aliases) if aliases else None,
            "repo_name": seed["repo_name"],
            "ecosystem": seed["ecosystem"],
            "package": seed["package"],
            "summary": vuln.get("summary"),
            "severity": severity_label,
            "cvss_vector": cvss_vector,
            "published": _parse_osv_dt(vuln.get("published")),
            "modified": _parse_osv_dt(vuln.get("modified")),
        }
