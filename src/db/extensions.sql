-- Run this BEFORE db:migrate.
-- Installs PostgreSQL extensions that the schema depends on.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
