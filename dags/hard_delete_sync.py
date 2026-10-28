"""
Hard Delete Sync DAG
Runs daily at midnight to handle hard deletes
Deletes rows in BigQuery that no longer exist in source PostgreSQL
"""

import os
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


DEFAULT_ARGS = {
    'owner': 'kumparan_data_eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='hard_delete_sync',
    default_args=DEFAULT_ARGS,
    description='Daily sync to handle hard deletes from source',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['articles', 'deletes', 'daily'],
)
def hard_delete_sync():
    @task
    def extract_active_ids(**context):
        """
        Extract ONLY the id column from entire PostgreSQL articles table
        """
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        query = """
        SELECT id
        FROM articles
        ORDER BY id
        """
        
        df = pd.read_sql_query(query, engine)
        
        if df.empty:
            raise AirflowSkipException("No new records found for this interval. Skipping downstream tasks.")
        
        temp_file = NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
        df.to_csv(temp_file.name, index=False)
        temp_file.close()
        
        print(f"Extracted {len(df)} active IDs. Saved to {temp_file.name}")
        return temp_file.name
    
    @task
    def load_active_ids_to_bq(extracted_file_path, **context):
        """
        Load active IDs into temporary BigQuery table kumparan_dwh.temp_active_ids
        """
        if not extracted_file_path or not os.path.exists(extracted_file_path):
            print("No file to load")
            return None
        
        df = pd.read_csv(extracted_file_path)
        
        if df.empty:
            os.unlink(extracted_file_path)
            return None
        
        # Use the service account key via environment variable
        from google.cloud import bigquery
        client = bigquery.Client()
        
        project_id = client.project
        dataset_id = 'kumparan_dwh'
        temp_table_ref = f"{project_id}.{dataset_id}.temp_active_ids"
        
        # Use LOAD job config for proper BigQuery integration
        from google.cloud.bigquery import LoadJobConfig, WriteDisposition
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        
        client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config, location='asia-southeast2').result()
        
        print(f"Loaded {len(df)} active IDs to {temp_table_ref}")
        
        return {
            'temp_ids_table': temp_table_ref,
            'temp_file': extracted_file_path
        }
    
    @task
    def delete_orphaned_rows(temp_info):
        """
        Delete rows in articles table where id does NOT exist in source PostgreSQL
        Reads active IDs directly from source so the task is robust to a missing
        temp table (e.g. after a retry where cleanup already dropped it).
        """
        # Use the service account key via environment variable
        from google.cloud import bigquery
        client = bigquery.Client()
        
        project_id = client.project
        dataset_id = 'kumparan_dwh'
        target_table_full = f"{project_id}.{dataset_id}.articles"
        
        # Ensure target table exists before LOAD
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{target_table_full}` (
            id INT64,
            title STRING,
            content STRING,
            published_at TIMESTAMP,
            author_id INT64,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            deleted_at TIMESTAMP
        );
        """
        client.query(create_table_sql, location='asia-southeast2').result()
        
        # Read active IDs directly from source PostgreSQL (robust to missing temp table)
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        import pandas as pd
        df_active = pd.read_sql_query("SELECT id FROM articles", engine)
        
        if df_active.empty:
            print("No active IDs found in source; skipping hard delete")
            return
        
        # Read target table into DataFrame
        df_target = client.query(f"SELECT * FROM `{target_table_full}`", location='asia-southeast2').to_dataframe()
        
        if df_target.empty:
            print("Target table is empty; nothing to delete")
            return

        orphaned_ids = set(df_target['id'].tolist()) - set(df_active['id'].tolist())
        if not orphaned_ids:
            raise AirflowSkipException("No orphaned rows found. Skipping downstream tasks.")

        # Filter: keep rows where id exists in source
        df_filtered = df_target[df_target['id'].isin(df_active['id'])]
        
        # Load filtered data back to target table with WRITE_TRUNCATE
        from google.cloud.bigquery import LoadJobConfig, WriteDisposition
        load_job_config = LoadJobConfig()
        load_job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        
        load_job = client.load_table_from_dataframe(df_filtered, target_table_full, job_config=load_job_config, location='asia-southeast2').result()
        
        print(f"Hard delete completed. Rows in target: {load_job.output_rows}")
    
    @task(trigger_rule='all_done')
    def cleanup_deletes(temp_info):
        """
        Clean up temporary files and tables
        """
        if not temp_info:
            return
        
        temp_file = temp_info.get('temp_file')
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
                print(f"Deleted temp file: {temp_file}")
            except Exception as e:
                print(f"Error deleting temp file: {e}")
        
        temp_ids_table = temp_info.get('temp_ids_table')
        if temp_ids_table:
            try:
                # Use the service account key via environment variable
                from google.cloud import bigquery
                client = bigquery.Client()
                client.delete_table(temp_ids_table, not_found_ok=True, location='asia-southeast2')
                print(f"Dropped temp table: {temp_ids_table}")
            except Exception as e:
                print(f"Error dropping temp table: {e}")
    
    extracted = extract_active_ids()
    loaded = load_active_ids_to_bq(extracted)
    deleted = delete_orphaned_rows(loaded)
    cleanup = cleanup_deletes(loaded)
    deleted >> cleanup


hard_delete_sync_dag = hard_delete_sync()
