# Airflow Connection Setup Instructions

After starting all containers with `docker-compose up -d`, you need to configure the connections for PostgreSQL and Google Cloud BigQuery.

## 1. Start the Airflow Environment

```bash
# Navigate to the project directory
cd kumparan_elt

# Initialize and start all services
docker-compose up -d
```

Wait for all containers to be healthy (especially `airflow-init` to complete). This may take a few minutes on first startup.

## 2. Access Airflow UI

Open your browser and navigate to:
```
http://localhost:8080
```

Default credentials (if not changed):
- Username: `airflow`
- Password: `airflow`

## 3. Configure Connections

### A. PostgreSQL Connection (`postgres_default`)

This connects Airflow to the existing `kumparan_postgres` source database.

#### Via Airflow UI:
1. Go to `Admin > Connections`
2. Click `+ Add a new record`
3. Fill in the following details:

| Field | Value |
|-------|-------|
| Connection Id | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `kumparan_postgres` (use container name for inter-container communication) |
| Schema | `kumparan_db` |
| Login | `admin` |
| Password | `adminpassword` |
| Port | `5432` |
| Extra | (leave empty) |

4. Click `Save`

#### Via CLI:
```bash
docker exec -it kumparan_airflow_webserver airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'kumparan_postgres' \
    --conn-schema 'kumparan_db' \
    --conn-login 'admin' \
    --conn-password 'adminpassword' \
    --conn-port '5432'
```

---

### B. Google Cloud Connection (`google_cloud_default`)

This connects Airflow to Google BigQuery. You need your Service Account JSON key file.

#### Preparation:
1. Place your Service Account JSON key in the project directory (e.g., `./config/service-account-key.json`)
2. Make sure the service account has the following roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`

#### Via Airflow UI:
1. Go to `Admin > Connections`
2. Click `+ Add a new record`
3. Fill in the following details:

| Field | Value |
|-------|-------|
| Connection Id | `google_cloud_default` |
| Connection Type | `Google Cloud` |
| Project Id | `your-gcp-project-id` |
| Keyfile Path | `/opt/airflow/config/service-account-key.json` |
| Keyfile JSON | (Alternatively paste the entire JSON content here) |
| Scopes | `https://www.googleapis.com/auth/cloud-platform` |

**Note:** Since we mount `./config` to `/opt/airflow/config`, use the container path.

4. Click `Save`

#### Via CLI:
```bash
docker exec -it kumparan_airflow_webserver airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"extra__google_cloud_platform__project": "your-gcp-project-id", "extra__google_cloud_platform__key_path": "/opt/airflow/config/service-account-key.json", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
```

Or if you want to use JSON directly:
```bash
docker exec -it kumparan_airflow_webserver airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"extra__google_cloud_platform__project": "your-gcp-project-id", "extra__google_cloud_platform__keyfile_dict": '"$(cat ./config/service-account-key.json | jq -c .)"'}'
```

---

## 4. Verify Setup

1. Go to `Admin > Connections` and verify both connections exist
2. Enable the DAGs in the DAGs view:
   - `hourly_articles_elt`
   - `historical_articles_backfill` 
   - `hard_delete_sync`
3. For historical backfill, with `catchup=True` and `start_date` from 2016, Airflow will schedule runs for each month. You may want to trigger manually or let it catch up based on your needs.
4. Monitor DAG runs in the Grid/Graph view

## 5. Important Notes

- Ensure the `kumparan_dwh` dataset exists in BigQuery (already created as per requirements)
- The target table `kumparan_dwh.articles` should have partitioning on `DATE(created_at)` and clustering on `id` (already created)
- Place your Google Cloud service account key in `./config/` directory before starting
- The source table `articles` must exist in `kumparan_db` (generated via `generate_data.py` if needed)
- All temporary files and staging tables are automatically cleaned up by the DAGs
- For production use, consider using Airflow Variables or Secret Manager for credentials
