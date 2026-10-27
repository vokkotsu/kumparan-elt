"""
Historical Backfill DAG for Articles (from 2016)
Runs with catchup=True but monthly schedule to chunk data extraction
Prevents overwhelming source database
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
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}


@dag(
    dag_id='historical_articles_backfill',
    default_args=DEFAULT_ARGS,
    description='Historical backfill for articles from 2016 - monthly chunks',
    schedule_interval='@monthly',  # Monthly chunks
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2023, 12, 31),
    catchup=True,
    max_active_runs=1,
    tags=['articles', 'backfill', 'historical'],
)
def historical_articles_backfill():
    @task
    def extract_historical_articles(**context):
        """
        Extract articles for the execution month based on created_at
        """
        execution_date = context['execution_date']
        # Extract for the entire month
        month_start = execution_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1)
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        query = """
        SELECT 
            id,
            title,
            content,
            published_at,
            author_id,
            created_at,
            updated_at,
            deleted_at
        FROM articles
        WHERE created_at >= %s 
          AND created_at < %s
        ORDER BY id
        """
        
        df = pd.read_sql_query(
            query,
            engine,
            params=(month_start.strftime('%Y-%m-%d %H:%M:%S'), month_end.strftime('%Y-%m-%d %H:%M:%S'))
        )
        
        print(f"Extracted {len(df)} rows")
        if df.empty:
            raise ValueError(f"No records found for {month_start.strftime('%Y-%m')}")
            return None
        
        temp_file = NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8')
        df.to_json(temp_file.name, orient='records', date_format='iso')
        temp_file.close()
        
        print(f"Extracted {len(df)} records for {month_start.strftime('%Y-%m')}. Saved to {temp_file.name}")
        return temp_file.name
    
    @task
    def load_to_staging_historical(extracted_file_path, **context):
        """
        Load extracted historical data into BigQuery staging table
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
        staging_table_id = f"staging_articles_backfill_{context['ts_nodash']}"
        table_ref = f"{project_id}.{dataset_id}.{staging_table_id}"
        
        # Convert timestamps
        for col in ['published_at', 'created_at', 'updated_at', 'deleted_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        job_config = bigquery_hook.job_config
        job_config.write_disposition = 'WRITE_TRUNCATE'
        
        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        
        print(f"Loaded {len(df)} records to staging table {table_ref}")
        
        return {
            'staging_table': table_ref,
            'temp_file': extracted_file_path
        }
    
    @task
    def merge_to_target_historical(staging_info):
        """
        Perform MERGE for historical data
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
        
        merge_query = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET
            title = S.title,
            content = S.content,
            published_at = S.published_at,
            author_id = S.author_id,
            created_at = S.created_at,
            updated_at = S.updated_at,
            deleted_at = S.deleted_at
        WHEN NOT MATCHED THEN
          INSERT (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
          VALUES (S.id, S.title, S.content, S.published_at, S.author_id, S.created_at, S.updated_at, S.deleted_at)
        """
        
        query_job = client.query(merge_query)
        query_job.result()
        
        print(f"Historical merge completed. Rows affected: {query_job.num_dml_affected_rows}")
    
    @task
    def cleanup_historical(staging_info):
        """
        Clean up temporary files and staging tables
        """
        if not staging_info:
            return
        
        temp_file = staging_info.get('temp_file')
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
                print(f"Deleted temp file: {temp_file}")
            except Exception as e:
                print(f"Error deleting temp file: {e}")
        
        staging_table = staging_info.get('staging_table')
        if staging_table:
            try:
                bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
                client = bigquery_hook.get_client()
                client.delete_table(staging_table, not_found_ok=True)
                print(f"Dropped staging table: {staging_table}")
            except Exception as e:
                print(f"Error dropping staging table: {e}")
    
    extracted = extract_historical_articles()
    staged = load_to_staging_historical(extracted)
    merged = merge_to_target_historical(staged)
    cleanup_historical(staged)


historical_articles_backfill_dag = historical_articles_backfill()
