# Metabase: дашборды Repo Radar 

### Топ репозиториев по приросту

```sql
SELECT
    repo_name,
    growth_score
FROM repo_radar.gold_rising_repos
ORDER BY growth_score DESC
LIMIT 15
```

### Таблица топа 

```sql
SELECT
  repo_name       AS `Репозиторий`,
  language        AS `Язык`,
  new_stars_7d    AS `Watch за 7 дней`,
  new_forks_7d    AS `Fork за 7 дней`,
  growth_score    AS `Прирост`,
  total_stars     AS `Звёзды всего`
FROM repo_radar.gold_rising_repos
ORDER BY growth_score DESC
LIMIT 25
```

### Прирост по языкам

```sql
SELECT
    coalesce(language, 'Unknown') AS language,
    sum(growth_score)             AS growth_score
FROM repo_radar.gold_rising_repos
GROUP BY language
ORDER BY growth_score DESC
```

### События по языкам во времени

```sql
SELECT
    event_date,
    language,
    total_events
FROM repo_radar.gold_language_trends
ORDER BY event_date, language
```

### Активные репозитории по языкам

```sql
SELECT
    event_date,
    language,
    active_repos
FROM repo_radar.gold_language_trends
ORDER BY event_date, language
```

### Топ репозиториев по событиям

```sql
SELECT
    repo_name,
    sum(total_events) AS total_events
FROM repo_radar.gold_repo_daily
GROUP BY repo_name
ORDER BY total_events DESC
LIMIT 15
```

### События по типам во времени

```sql
SELECT *
FROM (
    SELECT
        event_date,
        'Watch' AS event_type,
        sum(watch_events) AS events
    FROM repo_radar.gold_repo_daily
    GROUP BY event_date

    UNION ALL
    SELECT
        event_date,
        'Fork' AS event_type,
        sum(fork_events) AS events
    FROM repo_radar.gold_repo_daily
    GROUP BY event_date

    UNION ALL
    SELECT
        event_date,
        'Push' AS event_type,
        sum(push_events) AS events
    FROM repo_radar.gold_repo_daily
    GROUP BY event_date

    UNION ALL
    SELECT
        event_date,
        'Pull Request' AS event_type,
        sum(pr_events) AS events
    FROM repo_radar.gold_repo_daily
    GROUP BY event_date

    UNION ALL
    SELECT
        event_date,
        'Issues' AS event_type,
        sum(issue_events) AS events
    FROM repo_radar.gold_repo_daily
    GROUP BY event_date

    UNION ALL
    SELECT
        event_date,
        'Release' AS event_type,
        sum(release_events) AS events
    FROM repo_radar.gold_repo_daily
    GROUP BY event_date
)
ORDER BY event_date, event_type
```

### Доля типов

```sql
SELECT event_type, sum(events) AS events
FROM (
    SELECT 'Watch'        AS event_type, sum(watch_events)    AS events FROM repo_radar.gold_repo_daily
    UNION ALL
    SELECT 'Fork'         AS event_type, sum(fork_events)     AS events FROM repo_radar.gold_repo_daily
    UNION ALL
    SELECT 'Push'         AS event_type, sum(push_events)     AS events FROM repo_radar.gold_repo_daily
    UNION ALL
    SELECT 'Pull Request' AS event_type, sum(pr_events)       AS events FROM repo_radar.gold_repo_daily
    UNION ALL
    SELECT 'Issues'       AS event_type, sum(issue_events)    AS events FROM repo_radar.gold_repo_daily
    UNION ALL
    SELECT 'Release'      AS event_type, sum(release_events)  AS events FROM repo_radar.gold_repo_daily
)
GROUP BY event_type
ORDER BY events DESC
```



















