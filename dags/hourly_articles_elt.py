"""
Hourly Incremental Load DAG for Articles
Extracts updated articles from PostgreSQL within Airflow data interval,
loads to BigQuery staging table, and performs MERGE into target table.
"""

import os
import json
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import pandas as pd
import pendulum
from sqlalchemy import create_engine

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
    dag_id='hourly_articles_elt',
    default_args=DEFAULT_ARGS,
    description='Hourly incremental ELT for articles table',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['articles', 'incremental', 'hourly'],
)
def hourly_articles_elt():
    @task
    def extract_articles(**context):
        """
        Extract rows from PostgreSQL where updated_at falls within the data interval
        """
        logical_date = context['logical_date']
        lower_bound = logical_date - pendulum.duration(hours=1)
        lower_bound_str = lower_bound.strftime('%Y-%m-%d %H:%M:%S')

        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()

        query = """
        SELECT *
        FROM articles
        WHERE updated_at >= %s
        """

        df = pd.read_sql_query(
            query,
            engine,
            params=(lower_bound_str,)
        )
        
        if df.empty:
            raise AirflowSkipException("No new records found for this interval. Skipping downstream tasks.")
        
        # Save to temporary file
        temp_file = NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8')
        df.to_json(temp_file.name, orient='records', date_format='iso')
        temp_file.close()
        
        print(f"Extracted {len(df)} records. Saved to {temp_file.name}")
        return temp_file.name
    
    @task
    def load_to_staging(extracted_file_path, **context):
        """
        Load extracted data into temporary staging table in BigQuery
        """
        if not extracted_file_path or not os.path.exists(extracted_file_path):
            print("No file to load")
            return None
        
        df = pd.read_json(extracted_file_path, orient='records')
        
        if df.empty:
            os.unlink(extracted_file_path)
            return None
        
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bigquery_hook.get_client()
        
        project_id = client.project
        dataset_id = 'kumparan_dwh'
        staging_table_id = f"staging_articles_{context['ts_nodash']}"
        table_ref = f"{project_id}.{dataset_id}.{staging_table_id}"
        
        # Convert timestamps
        for col in ['published_at', 'created_at', 'updated_at', 'deleted_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Load to BigQuery
        from google.cloud.bigquery import LoadJobConfig, WriteDisposition
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        
        client.load_table_from_dataframe(df, table_ref, job_config=job_config, location='asia-southeast2').result()
        
        print(f"Loaded {len(df)} records to staging table {table_ref}")
        
        # Store both staging table ref and original file for cleanup
        return {
            'staging_table': table_ref,
            'temp_file': extracted_file_path
        }
    
    @task
    def merge_to_target(staging_info, **context):
        """
        Perform MERGE to upsert data from staging to target table
        """
        if not staging_info or not staging_info.get('staging_table'):
            print("No staging table to merge")
            if staging_info and staging_info.get('temp_file') and os.path.exists(staging_info['temp_file']):
                os.unlink(staging_info['temp_file'])
            return
        
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bigquery_hook.get_client()
        
        project_id = client.project
        dataset_id = 'kumparan_dwh'
        target_table = f"{project_id}.{dataset_id}.articles"
        staging_table = staging_info['staging_table']
        
        # Ensure target table exists before LOAD
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{target_table}` (
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
        
        # DDL-based upsert (deduplication) to avoid duplicates on re-runs
        upsert_query = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT id, title, content, published_at, author_id, created_at, updated_at, deleted_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
            FROM (
                SELECT id, title, content, CAST(published_at AS TIMESTAMP) AS published_at, author_id, CAST(created_at AS TIMESTAMP) AS created_at, CAST(updated_at AS TIMESTAMP) AS updated_at, CAST(deleted_at AS TIMESTAMP) AS deleted_at
                FROM `{target_table}`
                UNION ALL
                SELECT id, title, content, CAST(published_at AS TIMESTAMP) AS published_at, author_id, CAST(created_at AS TIMESTAMP) AS created_at, CAST(updated_at AS TIMESTAMP) AS updated_at, CAST(deleted_at AS TIMESTAMP) AS deleted_at
                FROM `{staging_table}`
            )
        )
        WHERE rn = 1;
        """
        client.query(upsert_query, location='asia-southeast2').result()
        
        print(f"Merge completed. Upserted data into {target_table}.")
    
    @task
    def cleanup(staging_info):
        """
        Clean up temporary files and staging tables
        """
        if not staging_info:
            return
        
        # Remove temp file
        temp_file = staging_info.get('temp_file')
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
                print(f"Deleted temp file: {temp_file}")
            except Exception as e:
                print(f"Error deleting temp file: {e}")
        
        # Drop staging table
        staging_table = staging_info.get('staging_table')
        if staging_table:
            try:
                bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
                client = bigquery_hook.get_client()
                client.delete_table(staging_table, not_found_ok=True, location='asia-southeast2')
                print(f"Dropped staging table: {staging_table}")
            except Exception as e:
                print(f"Error dropping staging table: {e}")
    
    # Task dependencies
    extracted = extract_articles()
    staged = load_to_staging(extracted)
    merged = merge_to_target(staged)
    cleanup(staged)


hourly_articles_elt_dag = hourly_articles_elt()
