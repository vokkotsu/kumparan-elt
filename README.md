# Kumparan ELT Pipeline — Data Engineer Technical Challenge Solution

## 1. Project Overview

Kumparan is a fast-growing Indonesian media company (est. 2016) with millions of daily active users and content production scaling to 1M+ articles per day. The BI team needs analytics-ready data in a data warehouse to support reporting and decision-making.

This repository contains the solution to the **Kumparan Data Engineer technical challenge**: building a resilient, cost-efficient ELT pipeline that moves article data from a transactional PostgreSQL database into Google BigQuery, orchestrated by Apache Airflow.

**Tech Stack:**

| Layer | Technology |
|---|---|
| Source | PostgreSQL 15 (`articles` table) |
| Orchestration | Apache Airflow 2.7.3 (LocalExecutor, Dockerized) |
| Data Warehouse | Google BigQuery (`kumparan_dwh` dataset) |
| Runtime | Docker Compose |

**Repository Structure:**

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

## 2. Database Schema

Source table: `articles`

| Column | Type | Description |
|---|---|---|
| `id` | INT | Primary key |
| `title` | STRING | Article title |
| `content` | STRING | Article body |
| `published_at` | TIMESTAMP | Publication timestamp |
| `author_id` | INT | Author reference |
| `created_at` | TIMESTAMP | Row creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |
| `deleted_at` | TIMESTAMP | Soft-delete marker (NULL if active) |

Target table: `kumparan_dwh.articles` in BigQuery, partitioned on `DATE(created_at)` and clustered on `id`.

## 3. Pipeline Architecture (Answering the Challenge)

### 1. Hourly ELT Pipeline (Core Tasks 1 & 2) — `hourly_articles_elt`

- **Purpose:** Incrementally capture new and updated articles from PostgreSQL every hour and load them into BigQuery, keeping the warehouse near real-time for BI.
- **Schedule/Trigger:** Hourly (`0 * * * *`), `catchup=False`, `max_active_runs=1`.
- **How it Works:**
  1. `extract_articles` — Queries PostgreSQL for rows where `updated_at` falls within the Airflow data interval; saves them to a temporary JSON file.
  2. `load_to_staging` — Loads the extracted records into a BigQuery staging table (`staging_articles_<ts>`).
  3. `merge_to_target` — Reads the staging table and upserts (MERGE) records into the target `articles` table keyed on `id`, ensuring idempotency and preventing data duplication.
  4. `cleanup` — Deletes the temporary file and drops the staging table.

### 2. Handling Historical Data from 2016 (Bonus 1) — `historical_articles_backfill`

- **Purpose:** Backfill the entire `articles` history (dating back to 2016) into BigQuery so BI has a complete analytics-ready dataset.
- **Schedule/Trigger:** Monthly (`@monthly`), `catchup=True` from `2016-01-01` to `2026-12-31`, `max_active_runs=1`.
- **How it Works:**
  1. `extract_historical_articles` — Queries PostgreSQL for rows where `created_at` falls within the execution month; saves them to a temporary JSON file.
  2. `load_to_staging_historical` — Loads the extracted records into a BigQuery staging table (`staging_articles_backfill_<ts>`).
  3. `merge_to_target_historical` — Reads the staging table and upserts (MERGE) records into the target `articles` table keyed on `id`, ensuring idempotency and preventing data duplication.
  4. `cleanup_historical` — Deletes the temporary file and drops the staging table.
- **Strategy:** Instead of a single massive pull that could overwhelm the source database, the backfill is chunked month-by-month from 2016 to the current date, processing one month per DAG run sequentially.

### 3. Hard Delete Synchronization (Bonus 2) — `hard_delete_sync`

- **Purpose:** Sync rows that are physically deleted (hard deleted) in the source PostgreSQL into BigQuery, so the warehouse stays consistent with the source of truth without losing historical context for BI.
- **Schedule/Trigger:** Daily at midnight (`0 0 * * *`), `catchup=False`, `max_active_runs=1`.
- **How it Works:**
  1. `extract_active_ids` — Extracts only the `id` column from the entire PostgreSQL `articles` table.
  2. `load_active_ids_to_bq` — Loads the active IDs into a temporary BigQuery table (`temp_active_ids`).
  3. `delete_orphaned_rows` — Compares all `id`s in the source against the `id`s in BigQuery. For any ID present in BigQuery but missing in the source, executes a MERGE to update the `deleted_at` timestamp in BigQuery (Soft Delete in DWH), preserving historical data for BI without losing the deletion context.
  4. `cleanup_deletes` — Deletes the temporary file and drops the temporary IDs table.

## 4. Key Engineering Optimizations

- **Cost & Compute Efficiency (Short-Circuiting):** Implemented `AirflowSkipException` to short-circuit and skip downstream tasks when 0 rows are extracted, marking tasks as *Skipped* instead of processing empty data — saving compute and BigQuery costs.
- **Resiliency (Self-Healing Schema):** Uses `CREATE TABLE IF NOT EXISTS` before merging, so target/staging tables are created automatically if missing.
- **Idempotency:** Uses BigQuery MERGE (upsert based on `id`) to prevent duplicate rows even if DAGs are rerun.
- **Free-Tier Friendly:** The hard-delete sync avoids BigQuery DML (which requires billing) by using a read-filter-writeback approach.
- **Robust Retries:** Downstream tasks read source data directly, so retries succeed even if temporary tables were already cleaned up.
- **No `trigger_rule` overrides:** Downstream tasks rely on Airflow's default behavior and are automatically skipped when an upstream task is skipped.

## 5. Setup & Running

```bash
# Start all services
docker-compose up -d

# Configure Airflow connections (see SETUP_INSTRUCTIONS.md)
# Then enable the DAGs in the Airflow UI (http://localhost:8080)
```

Place your Google Cloud service account key in `./config/` before starting. The `kumparan_dwh` dataset and the `articles` target table (partitioned on `DATE(created_at)`, clustered on `id`) are assumed to already exist in BigQuery.

Temporary files and staging tables are automatically cleaned up by each DAG.
