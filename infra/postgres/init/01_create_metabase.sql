SELECT format('CREATE DATABASE %I', 'metabase')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'metabase'
)\gexec

