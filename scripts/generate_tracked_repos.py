"""
Генерирует config/tracked_repos.yml с топ-300 репозиториями: 100 Python + 100 Go + 100 Rust
"""

import httpx
import yaml
import os
from pathlib import Path
import time

from dotenv import load_dotenv  # pip install python-dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
OUTPUT_FILE = PROJECT_ROOT / "config" / "tracked_repos.yml"

ECOSYSTEM_MAP = {
    "Python": "pypi",
    "Go": "go",
    "Rust": "cargo",
}


def fetch_top_repos(language: str, per_page: int = 100, max_pages: int = 1) -> list[dict]:
    """
    Получить топ репозитории по звёздам
    GitHub API возвращает максимум 1000 результатов (10 страниц по 100)
    """
    url = "https://api.github.com/search/repositories"
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    
    all_repos = []
    
    for page in range(1, max_pages + 1):
        params = {
        "q": f"language:{language} fork:false",
        "sort": "stars",
        "order": "desc",
        "per_page": per_page,
        "page": page,
        }

        print(f"   Страница {page}/{max_pages}...", end=" ")
        
        try:
            response = httpx.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            items = data.get("items", [])
            all_repos.extend(items)
            
            print(f"✓ Получено {len(items)} репозиториев")
            
            # Rate limiting
            if "X-RateLimit-Remaining" in response.headers:
                remaining = int(response.headers["X-RateLimit-Remaining"])
                if remaining < 5:
                    print(f"   ⏳ Rate limit близок к исчерпанию ({remaining}), ждём 60 секунд...")
                    time.sleep(60)
            
            # Небольшая задержка между запросами
            time.sleep(1)
            
        except Exception as e:
            print(f"✗ Ошибка: {e}")
            break
    
    return all_repos


def extract_package_name(repo: dict) -> str:
    name = repo["name"]
    full_name = repo["full_name"]
    language = repo.get("language") or ""
    if language == "Python":
        return name.lower().replace("_", "-")
    if language == "Go":
        return f"github.com/{full_name}"

    return name.lower().replace("_", "-")


def main():
    print("🔍 Собираем топ-300: 100 Python + 100 Go + 100 Rust\n")
    if GITHUB_TOKEN:
        print("✓ Используем GITHUB_TOKEN")
    else:
        print("⚠️  GITHUB_TOKEN не найден — rate limit 10 запросов/минуту")

    LANGUAGES = ["Python", "Go", "Rust"]
    
    raw_repos = []
    for lang in LANGUAGES:
        print(f"\n▶ {lang}")
        raw_repos.extend(fetch_top_repos(language=lang, per_page=100, max_pages=1))
        
    print(f"\n📊 Получено {len(raw_repos)} репозиториев")
    
    repos_data = []
    seen = set()
    
    for repo in raw_repos:
        full_name = repo["full_name"]
        if full_name in seen:
            continue
        seen.add(full_name)
        
        owner, name = full_name.split("/")
        language = repo.get("language") or "Unknown"
        ecosystem = ECOSYSTEM_MAP.get(language, "unknown")
        stars = repo.get("stargazers_count", 0)
        
        repos_data.append({
            "owner": owner,
            "name": name,
            "ecosystem": ecosystem,
            "package": extract_package_name(repo),
            "language": language.lower() if language else "unknown",
            "stars": stars,
        })
        
        if len(repos_data) >= 300:
            break
    
    repos_data.sort(key=lambda x: x["stars"], reverse=True)
    
    for repo in repos_data:
        del repo["stars"]
    
    repos_data = repos_data[:300]
    
    lang_stats = {}
    for repo in repos_data:
        lang = repo["language"]
        lang_stats[lang] = lang_stats.get(lang, 0) + 1
    
    output = {
        "repositories": repos_data
    }
    
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        yaml.dump(output, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    print(f"\n✅ Сохранено {len(repos_data)} репозиториев в {OUTPUT_FILE}")
    print(f"\n📈 Статистика по языкам:")
    for lang, count in sorted(lang_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"   {lang.capitalize():<15} {count:>3} репозиториев")


if __name__ == "__main__":
    main()



