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
            print("No active IDs found in source")
            return None
        
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
        
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bigquery_hook.get_client()
        
        project_id = client.project
        dataset_id = 'kumparan_dwh'
        temp_table_ref = f"{project_id}.{dataset_id}.temp_active_ids"
        
        job_config = bigquery_hook.job_config
        job_config.write_disposition = 'WRITE_TRUNCATE'
        
        client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config).result()
        
        print(f"Loaded {len(df)} active IDs to {temp_table_ref}")
        
        return {
            'temp_ids_table': temp_table_ref,
            'temp_file': extracted_file_path
        }
    
    @task
    def delete_orphaned_rows(temp_info):
        """
        Delete rows in articles table where id does NOT exist in temp_active_ids
        """
        if not temp_info or not temp_info.get('temp_ids_table'):
            print("No temp active IDs table to compare")
            if temp_info and temp_info.get('temp_file') and os.path.exists(temp_info['temp_file']):
                os.unlink(temp_info['temp_file'])
            return
        
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bigquery_hook.get_client()
        
        project_id = client.project
        dataset_id = 'kumparan_dwh'
        target_table = f"{project_id}.{dataset_id}.articles"
        temp_ids_table = temp_info['temp_ids_table']
        
        delete_query = f"""
        DELETE FROM `{target_table}`
        WHERE id NOT IN (
            SELECT id FROM `{temp_ids_table}`
        )
        """
        
        query_job = client.query(delete_query)
        query_job.result()
        
        print(f"Hard delete completed. Rows affected: {query_job.num_dml_affected_rows}")
    
    @task
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
                bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
                client = bigquery_hook.get_client()
                client.delete_table(temp_ids_table, not_found_ok=True)
                print(f"Dropped temp table: {temp_ids_table}")
            except Exception as e:
                print(f"Error dropping temp table: {e}")
    
    extracted = extract_active_ids()
    loaded = load_active_ids_to_bq(extracted)
    deleted = delete_orphaned_rows(loaded)
    cleanup_deletes(loaded)


hard_delete_sync_dag = hard_delete_sync()
