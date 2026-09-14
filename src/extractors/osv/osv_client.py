"""
OSV.dev клиент: уязвимости по (ecosystem, package) из seed.
Два шага:
  * POST /v1/querybatch — по списку пакетов, отдаёт только id уязвимостей
  * GET  /v1/vulns/{id} — полный объект (severity, published, aliases)
API публичный, токен не нужен. Вежливый rate limit через backoff.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

from config.paths import OSV_CACHE_DIR
from extractors.osv.cache import cache_get, cache_put
from utils.collections import chunked
from utils.datetime_parse import parse_iso_utc_naive
from utils.http_retry import request_json_with_retry

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
        return request_json_with_retry(
            self.http,
            method,
            url,
            json_body=json_body,
            max_retries=MAX_RETRIES,
            label="OSV",
        )

    def fetch_advisories(
        self,
        repos: list[dict],
        max_workers: int = 8,
        cache_dir: Path | None = OSV_CACHE_DIR,
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
            cached = cache_get(cache_dir, vid, vuln_modified.get(vid))
            if cached is not None:
                details[vid] = cached
            else:
                to_fetch.append(vid)

        print(f"   кэш: {total - len(to_fetch)}/{total} готово, тянем {len(to_fetch)}")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for n, (vid, data) in enumerate(pool.map(self._fetch_vuln, to_fetch), start=1):
                details[vid] = data
                cache_put(cache_dir, vid, data)
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
            "published": parse_iso_utc_naive(vuln.get("published")),
            "modified": parse_iso_utc_naive(vuln.get("modified")),
        }
