# NYC Motor Vehicle Collisions — Data Pipeline

An end-to-end batch data pipeline for NYC Motor Vehicle Collisions data, built
with a medallion architecture on Databricks + Delta Lake, transformed with dbt,
and orchestrated with GitHub Actions.

It pulls collision data from the NYC Open Data API every day, lands it in a
Bronze layer, cleans and deduplicates it into Silver, and models it into a Gold
star schema ready for analytics and BI.

## Architecture at a glance

```
NYC Open Data API  ->  UC Volume  ->  Bronze  ->  Silver  ->  Gold (star schema)  ->  Power BI
   (Python)            (JSONL)       (raw)       (clean)     (dbt models + tests)
```

- **Bronze / Silver** run as SQL on a Databricks **Serverless SQL Warehouse**.
- **Gold** is built with **dbt**.
- Daily run is orchestrated by **GitHub Actions** (cron).

See [`docs/architecture.md`](docs/architecture.md) for the full design and the
reasoning behind each decision.

## Tech stack

| Concern | Tool |
|---|---|
| Source | NYC Open Data SODA2 API (`h9gi-nx95`, no API key needed) |
| Ingestion | Python (`requests`, `databricks-sdk`) |
| Storage & compute | Databricks Unity Catalog + Delta Lake + Serverless SQL Warehouse |
| Bronze + Silver | SQL files run on the warehouse |
| Gold | dbt (`dbt-databricks`) |
| Orchestration | GitHub Actions (cron) |
| BI | Power BI (optional, connects to the warehouse) |

## Repository structure

```
ingestion/
  config.py              Settings (API URL, page size, table names, paths)
  utils.py               Logging + watermark load/save
  fetch_collisions.py    Fetch from API, flatten, upload JSONL to the Volume
  state/                 Watermark file (last_run_state.json, gitignored)
databricks/
  run_sql.py             Runs a .sql file on the Serverless Warehouse
  sql/
    00_setup.sql         Create schemas + the landing Volume
    01_bronze.sql        COPY INTO Bronze + OPTIMIZE
    02_silver.sql        Clean / type / dedupe into Silver
dbt/
  dbt_project.yml        dbt project config
  profiles.yml           Databricks connection (uses env vars)
  packages.yml           dbt_utils dependency
  macros/                Schema-naming override
  seeds/                 borough_lookup.csv reference data
  models/
    staging/             stg_collisions (reads Silver) + source defs + tests
    gold/                dim_date, dim_location, dim_vehicle, fact_collisions,
                         monthly/borough metrics + tests + docs
.github/workflows/
  pipeline.yml           Daily pipeline (ingest -> bronze -> silver -> dbt)
  dbt_ci.yml             PR check (dbt compile)
docs/
  architecture.md        Full architecture write-up
```

## Prerequisites

- Python 3.11+
- A Databricks workspace with a **Serverless SQL Warehouse**
  (works on Databricks Free Edition)
- A Databricks personal access token

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

You need three values:

| Variable | Where to find it |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL, e.g. `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Settings → Developer → Access tokens → Generate new token |
| `DATABRICKS_HTTP_PATH` | SQL Warehouses → your warehouse → Connection details → HTTP path |

Then export them into your shell (PowerShell example):

```powershell
$env:DATABRICKS_HOST="https://dbc-xxxx.cloud.databricks.com"
$env:DATABRICKS_TOKEN="dapi..."
$env:DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxxxxxxxxxxx"
```

> The warehouse ID is parsed from the end of `DATABRICKS_HTTP_PATH`, so there's
> no separate cluster or warehouse-id variable to set.

## Running the pipeline

Run the stages in order:

```bash
# 1. Create schemas + the landing Volume (idempotent, safe to re-run)
python databricks/run_sql.py databricks/sql/00_setup.sql

# 2. Fetch from the API and upload to the Volume
python ingestion/fetch_collisions.py
#    First run pulls 1 year of data. To force a full re-pull later:
#    python ingestion/fetch_collisions.py --full-reload

# 3. Load Bronze (COPY INTO)
python databricks/run_sql.py databricks/sql/01_bronze.sql

# 4. Build Silver (clean + dedupe)
python databricks/run_sql.py databricks/sql/02_silver.sql

# 5. Build and test the Gold layer with dbt
cd dbt
dbt deps
dbt seed
dbt run
dbt test
```

After this you'll have:

```
workspace.bronze.collisions_raw
workspace.silver.collisions_clean
workspace.staging.stg_collisions
workspace.gold.dim_date
workspace.gold.dim_location
workspace.gold.dim_vehicle
workspace.gold.fact_collisions
workspace.gold.monthly_collision_metrics
workspace.gold.borough_collision_metrics
```

## Daily automation (GitHub Actions)

The pipeline runs automatically every day via
[`.github/workflows/pipeline.yml`](.github/workflows/pipeline.yml).

To enable it, add these as **repository secrets**
(Settings → Secrets and variables → Actions):

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_HTTP_PATH`

You can also trigger it manually from the Actions tab, with an optional
`full_reload` input.

## Data quality

dbt tests run on every build — not-null, unique, relationships between fact and
dimensions, accepted values, and non-negative count checks. Failing rows are
stored as tables (`dbt test --store-failures`) so you can inspect exactly what
broke.

## Connecting Power BI

Point Power BI at the Serverless SQL Warehouse using the Databricks connector
and the same host + HTTP path, then build reports on the `workspace.gold.*`
tables. The metrics tables (`monthly_collision_metrics`,
`borough_collision_metrics`) are pre-aggregated for fast dashboards.

## Notes / assumptions

- The NYC Open Data API has a reporting lag of about 5 days, so the most recent
  few days won't have data yet. This is expected, not a bug.
- The initial load pulls **1 year** of data (`INITIAL_LOAD_DAYS` in
  `ingestion/config.py`).
- Everything is idempotent — any stage is safe to re-run without creating
  duplicates.
