# Architecture

This document explains how the NYC Collisions data pipeline is put together,
why it's built this way, and what each piece is responsible for.

## Overview

The pipeline pulls NYC Motor Vehicle Collisions data from the NYC Open Data API
once a day and turns it into clean, analytics-ready tables using a
**medallion architecture** (Bronze → Silver → Gold).

```
NYC Open Data API (SODA2)
        |
        |  Python (requests, pagination, incremental)
        v
  UC Volume (raw JSONL files)
        |
        |  COPY INTO (SQL on Serverless Warehouse)
        v
  Bronze: workspace.bronze.collisions_raw      (raw, all STRING)
        |
        |  CREATE OR REPLACE TABLE AS SELECT (SQL on Serverless Warehouse)
        v
  Silver: workspace.silver.collisions_clean    (typed, cleaned, deduplicated)
        |
        |  dbt (Serverless Warehouse)
        v
  Gold: workspace.gold.*                        (star schema + metrics)
        |
        v
  Power BI / Databricks SQL
```

## Platform note: Databricks Free Edition

This project runs on **Databricks Free Edition**, which shaped a few key choices:

- **No all-purpose clusters.** Free Edition does not give you a cluster you can
  run PySpark notebooks on. It *does* give you a **Serverless SQL Warehouse**.
  So Bronze and Silver are written as **SQL files executed on the warehouse**
  (via the SQL Statement Execution API), not as PySpark notebooks.
- **Catalog is `workspace`.** You can't create new top-level catalogs in Free
  Edition, so every schema lives under the built-in `workspace` catalog.
- **Unity Catalog Volumes are available**, so we use a Volume as the landing
  zone for raw files.

This is a realistic constraint to design around, and the SQL-on-serverless
approach is a perfectly valid production pattern.

## Layer responsibilities

### Ingestion (`ingestion/fetch_collisions.py`)
- Reads the watermark (last `crash_date` loaded) from a local JSON state file.
- Fetches records where `crash_date > watermark` and `crash_date <= yesterday`.
  We never ingest "today" because the current day is incomplete.
- Handles **pagination** (the API caps responses at 50,000 rows per request).
- Handles **API failures** with a retry loop and exponential backoff.
- **Flattens** the nested `location` field to a JSON string (otherwise the
  COPY INTO into a STRING column fails).
- Uploads the batch as a JSONL file to the UC Volume.
- Advances the watermark to the max `crash_date` actually fetched — only after a
  successful upload. If anything fails, the watermark is untouched so the next
  run retries the same window.

### Bronze (`databricks/sql/01_bronze.sql`)
- Raw landing zone. Every column is **STRING** — no casting, no cleaning.
- `COPY INTO` loads new files from the Volume. COPY INTO tracks which files it
  has already loaded, so re-running is **idempotent** (no duplicate rows).
- `crash_date_partition` is a **generated column** and `_ingestion_timestamp`
  is a **column default**, so COPY INTO fills both automatically.
- `mergeSchema` is on, so records missing some fields don't break the load.
- Table is partitioned by `crash_date_partition` for efficient date pruning.

### Silver (`databricks/sql/02_silver.sql`)
- Reads Bronze and produces a clean, typed, deduplicated table.
- **Type casting**: strings → DATE / TIMESTAMP / DOUBLE / INT.
- **Cleaning**: uppercases and trims boroughs, street names, vehicle types;
  empty strings → NULL; `(0,0)` coordinates → NULL; negative counts → NULL.
- **Filtering**: drops rows with no `collision_id` or no `crash_date`.
- **Deduplication**: `ROW_NUMBER()` over `collision_id`, keeping the most
  recently ingested version.
- **Computed columns**: `total_injuries`, `total_fatalities`, `has_injuries`,
  `has_fatalities`.
- Rebuilt fully each run with `CREATE OR REPLACE TABLE AS SELECT`. The dataset is
  small (about a year of data), so a full rebuild is simpler and more robust than
  an incremental merge, and it's idempotent by definition.

### Gold (`dbt/models/gold/`)
A **star schema** built with dbt:

- `dim_date` — one row per calendar day (date spine, 2022–2030).
- `dim_location` — distinct borough + zip code combinations.
- `dim_vehicle` — distinct vehicle-type combinations.
- `fact_collisions` — one row per collision, with foreign keys to the
  dimensions and all the injury/fatality measures.
- `monthly_collision_metrics` — pre-aggregated metrics by year + month.
- `borough_collision_metrics` — pre-aggregated metrics by borough.

A staging model (`stg_collisions`) sits between Silver and Gold to rename
columns into a consistent form (the source has inconsistent vehicle column
names like `vehicle_type_code1` vs `vehicle_type_code_3`).

## Why dbt only does Gold

Bronze and Silver are mechanical (load raw, cast, clean). The Gold layer is
where the actual **analytics modeling** happens — building a star schema,
defining grain, writing tests, documenting columns. That's exactly what dbt is
good at, so dbt owns Gold and nothing else.

## Data quality

dbt tests run after every build:
- **not_null** / **unique** on keys and surrogate keys.
- **relationships** tests verifying every fact row's foreign keys exist in the
  dimensions.
- **accepted_values** on `borough`, `month`, etc.
- **expression** tests (e.g. injury counts must be `>= 0`).

`dbt test --store-failures` saves any failing rows as tables so you can query
exactly what broke instead of digging through logs.

## Orchestration

A GitHub Actions workflow (`.github/workflows/pipeline.yml`) runs daily on a
cron schedule and executes the steps in order:

```
00_setup.sql -> fetch_collisions.py -> 01_bronze.sql -> 02_silver.sql
   -> dbt deps -> dbt run -> dbt test
```

Each step only runs if the previous one succeeded, so bad data never gets
promoted downstream. A second workflow (`dbt_ci.yml`) runs `dbt compile` on
pull requests that touch the `dbt/` folder, to catch broken models before merge.

## Idempotency & recovery

Every stage is safe to re-run:
- **Ingestion** only advances the watermark on success.
- **Bronze** COPY INTO skips files it has already loaded.
- **Silver** fully rebuilds from Bronze.
- **Gold** dbt models are rebuilt each run.

If a run fails halfway, just run it again — you won't get duplicates or
half-loaded data.

## Connection model

Everything authenticates to Databricks with three environment variables:

| Variable | Used by | Purpose |
|---|---|---|
| `DATABRICKS_HOST` | run_sql.py, fetch, dbt | Workspace URL |
| `DATABRICKS_TOKEN` | run_sql.py, fetch, dbt | Personal access token |
| `DATABRICKS_HTTP_PATH` | run_sql.py, dbt | SQL Warehouse path (the warehouse ID is parsed from the end of it) |

There is no separate cluster ID, because there is no cluster — all compute is
the Serverless SQL Warehouse.
