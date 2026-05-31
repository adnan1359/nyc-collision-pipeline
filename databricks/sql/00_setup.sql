-- One-time (idempotent) setup.
-- Creates the schemas and the landing Volume inside the built-in "workspace"
-- catalog. This must run BEFORE fetch_collisions.py, because the script uploads
-- JSONL files into the Volume created here.

-- Schema for the landing Volume where raw JSON files arrive
CREATE SCHEMA IF NOT EXISTS workspace.landing;

-- The Volume itself - fetch_collisions.py uploads here, Bronze reads from here
CREATE VOLUME IF NOT EXISTS workspace.landing.raw;

-- Bronze and Silver schemas
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
