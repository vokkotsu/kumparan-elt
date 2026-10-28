# Kumparan ELT Pipeline

An Apache Airflow-based ELT (Extract, Load, Transform) pipeline that synchronizes article data from a source PostgreSQL database into Google BigQuery. The pipeline is composed of three DAGs covering historical backfill, hourly incremental loads, and daily hard-delete synchronization.

## Architecture Overview

```
Source PostgreSQL (articles)
        │
        ├──► Historical Backfill (monthly) ──► BigQuery staging ──► BigQuery target (articles)
        ├──► Hourly Incremental (hourly)   ──► BigQuery staging ──► BigQuery target (articles)
        └──► Hard Delete Sync (daily)      ──► BigQuery target (articles)  [removes orphaned rows]
```

- **Source**: PostgreSQL `articles` table (container `kumparan_postgres`).
- **Target**: BigQuery table `kumparan_dwh.articles` (partitioned on `DATE(created_at)`, clustered on `id`).
- **Orchestration**: Apache Airflow 2.7.3 (LocalExecutor), running in Docker.

## Prerequisites

- Docker & Docker Compose
- A Google Cloud service account key with `BigQuery Data Editor` and `BigQuery Job User` roles, placed in `./config/`
- PostgreSQL source database populated (use `generate_data.py` if needed)

## Quick Start

```bash
# Start all services
docker-compose up -d

# Configure Airflow connections (see SETUP_INSTRUCTIONS.md)
# Then enable the DAGs in the Airflow UI (http://localhost:8080)
```

## DAGs

### 1. `historical_articles_backfill`

- **Purpose/Objective:** Backfill the entire `articles` history from 2016 into BigQuery in monthly chunks, avoiding overwhelming the source database by processing one month at a time.
- **Schedule/Trigger:** Monthly (`@monthly`), with `catchup=True` from `2016-01-01` to `2026-12-31`. `max_active_runs=1` ensures sequential processing.
- **How it Works (Pipeline Architecture):**
  1. `extract_historical_articles` — Queries PostgreSQL for rows where `created_at` falls within the execution month, saves them to a temporary JSON file.
  2. `load_to_staging_historical` — Loads the extracted records into a BigQuery staging table (`staging_articles_backfill_<ts>`).
  3. `merge_to_target_historical` — Reads the staging table and upserts (MERGE) records into the target `articles` table keyed on `id`, ensuring idempotency and preventing data duplication.
  4. `cleanup_historical` — Deletes the temporary file and drops the staging table.
- **Key Features & Optimizations:**
  - **Create Table If Not Exists** — Self-healing schema creation for the target table.
  - **Short-Circuiting** (`AirflowSkipException`) — Skips downstream tasks when 0 rows are extracted, saving compute and BigQuery costs.
  - **Idempotency** — Uses MERGE/upsert semantics keyed on `id` to prevent data duplication on re-runs.

### 2. `hourly_articles_elt`

- **Purpose/Objective:** Incrementally capture new and updated articles from PostgreSQL every hour and upsert them into BigQuery, keeping the warehouse near real-time.
- **Schedule/Trigger:** Hourly (`0 * * * *`), `catchup=False`.
- **How it Works (Pipeline Architecture):**
  1. `extract_articles` — Queries PostgreSQL for rows where `updated_at` falls within the Airflow data interval, saves them to a temporary JSON file.
  2. `load_to_staging` — Loads the extracted records into a BigQuery staging table (`staging_articles_<ts>`).
  3. `merge_to_target` — Reads the staging table and upserts (MERGE) records into the target `articles` table keyed on `id`, ensuring idempotency and preventing data duplication.
  4. `cleanup` — Deletes the temporary file and drops the staging table.
- **Key Features & Optimizations:**
  - **Create Table If Not Exists** — Self-healing schema creation for the target table.
  - **Short-Circuiting** (`AirflowSkipException`) — Skips downstream tasks when 0 rows are extracted, saving compute and BigQuery costs.
  - **Idempotency** — Uses MERGE/upsert semantics keyed on `id` to prevent data duplication on re-runs.

### 3. `hard_delete_sync`

- **Purpose/Objective:** Detect rows that no longer exist in the source PostgreSQL (hard deletes) and remove them from BigQuery, keeping the warehouse in sync with the source of truth.
- **Schedule/Trigger:** Daily at midnight (`0 0 * * *`), `catchup=False`.
- **How it Works (Pipeline Architecture):**
  1. `extract_active_ids` — Extracts only the `id` column from the entire PostgreSQL `articles` table.
  2. `load_active_ids_to_bq` — Loads the active IDs into a temporary BigQuery table (`temp_active_ids`).
  3. `delete_orphaned_rows` — Reads the target table and the active IDs, filters out orphaned rows (ids not present in source), and writes the filtered result back with `WRITE_TRUNCATE`.
  4. `cleanup_deletes` — Deletes the temporary file and drops the temporary IDs table.
- **Key Features & Optimizations:**
  - **Create Table If Not Exists** — Self-healing schema creation for the target table.
  - **Short-Circuiting** (`AirflowSkipException`) — Skips downstream tasks when 0 active IDs are found, saving compute and BigQuery costs.
  - **Free-Tier Friendly** — Avoids BigQuery DML (which requires billing); uses a read-filter-writeback approach instead.
  - **Robust Retries** — Reads active IDs directly from PostgreSQL, so retries succeed even if the temporary table was already cleaned up.

## Shared Optimizations

- **AirflowSkipException** short-circuits empty extractions, marking tasks as *Skipped* (yellow) instead of *Failed* (red) or wastefully processing empty data.
- **No `trigger_rule` overrides** — downstream tasks rely on Airflow's default behavior and are automatically skipped when an upstream task is skipped.
- **Connection reuse** — PostgreSQL via `PostgresHook(postgres_conn_id='postgres_default')`, BigQuery via `BigQueryHook` / `bigquery.Client()`.

## Repository Structure

```
kumparan_elt/
├── dags/                          # Airflow DAG definitions
│   ├── historical_articles_backfill.py
│   ├── hourly_articles_elt.py
│   └── hard_delete_sync.py
├── config/                        # GCP service account keys (gitignored)
├── docker-compose.yml             # Airflow + Postgres services
├── SETUP_INSTRUCTIONS.md          # Connection setup guide
├── generate_data.py               # Source data generator
└── README.md
```

## Notes

- Place your Google Cloud service account key in `./config/` before starting (see `SETUP_INSTRUCTIONS.md`).
- The `kumparan_dwh` dataset and the `articles` target table (partitioned on `DATE(created_at)`, clustered on `id`) are assumed to already exist in BigQuery.
- Temporary files and staging tables are automatically cleaned up by each DAG.
