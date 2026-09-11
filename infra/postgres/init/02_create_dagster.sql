SELECT format('CREATE DATABASE %I', 'dagster')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'dagster'
)\gexec

