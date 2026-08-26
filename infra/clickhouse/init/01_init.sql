CREATE DATABASE IF NOT EXISTS repo_radar;

USE repo_radar;

CREATE TABLE IF NOT EXISTS health_check (
    id UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO health_check (id)
SELECT 1
WHERE NOT EXISTS (SELECT 1 FROM health_check WHERE id = 1);


