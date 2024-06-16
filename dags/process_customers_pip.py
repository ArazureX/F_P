import os
import tempfile
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from datetime import datetime
import logging

GCS_BUCKET = 'final_project_ex'
GCS_PREFIX = 'customers/'
BRONZE_DATASET = 'bronze'
BRONZE_TABLE = 'customers'
SILVER_DATASET = 'silver'
SILVER_TABLE = 'sales'

BRONZE_TABLE_SCHEMA = [
    {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
]

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def load_latest_csv_to_bronze(**kwargs):
    gcs_hook = GCSHook()
    client = bigquery.Client()


    all_folders = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=GCS_PREFIX , delimiter='/')
    
    logging.info(f"All folders: {all_folders}")

    date_folders = [folder for folder in all_folders if folder.rstrip('/').split('/')[-1].replace('-', '').isdigit()]

    logging.info(f"Date folders: {date_folders}")

    if not date_folders:
        raise ValueError("No date folders found in the specified subfolder.")

    date_folders = [folder.rstrip('/').split('/')[-1] for folder in date_folders]
    latest_folder = max(date_folders, key=lambda x: datetime.strptime(x, '%Y-%m-%d'))

    logging.info(f"Latest folder: {latest_folder}")

    files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=f"{GCS_PREFIX}{latest_folder}/")

    logging.info(f"Files in the latest folder: {files}")

    csv_files = [file for file in files if file.endswith('.csv')]

    if not csv_files:
        raise ValueError(f"No CSV files found in the latest folder: {latest_folder}")

    logging.info(f"CSV files: {csv_files}")

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        for i, file in enumerate(csv_files):
            # Завантажуємо файл з GCS
            data = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=file).decode('utf-8')
            
            if i == 0:
                temp_file.write(data)
            else:
                temp_file.write('\n'.join(data.split('\n')[1:]))

        temp_file_path = temp_file.name
    
    hook = BigQueryHook()
    if hook.table_exists(project_id=client.project, dataset_id=BRONZE_DATASET, table_id=BRONZE_TABLE):
        client.delete_table(f"{client.project}.{BRONZE_DATASET}.{BRONZE_TABLE}")

    dataset_ref = client.dataset(BRONZE_DATASET)
    table_ref = dataset_ref.table(BRONZE_TABLE)

    load_job = client.load_table_from_file(
        open(temp_file_path, 'rb'),
        table_ref,
        job_config=bigquery.LoadJobConfig(
            schema=BRONZE_TABLE_SCHEMA,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )

    load_job.result()  

    os.remove(temp_file_path)

def transform_bronze_to_silver(**kwargs):
    client = bigquery.Client()

    query = f"""
    CREATE OR REPLACE TABLE `silver.customers` AS
    SELECT
    Id AS client_id,
    FirstName AS first_name,
    LastName AS last_name,
    Email AS email,
    CASE
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-\d{2}-\d{2}$') THEN CAST(RegistrationDate AS DATE FORMAT 'YYYY-MM-DD')
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-\w{3}-\d{2}$') THEN CAST(RegistrationDate AS DATE FORMAT 'YYYY-MON-DD')
            ELSE CAST(RegistrationDate AS DATE)
    END as registration_date,
    State AS state
    FROM `bronze.customers`;

    """

    query_job = client.query(query)
    query_job.result()

with models.DAG(
    'process_customers_pipeline',
    default_args=default_args,
    schedule_interval='0 1 * * *',  
) as dag:

    load_csvs_to_bronze_task = PythonOperator(
        task_id='load_latest_csv_to_bronze',
        python_callable=load_latest_csv_to_bronze,
        provide_context=True,
    )

    transform_data_to_silver_task = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver,
        provide_context=True,
    )

    load_csvs_to_bronze_task >> transform_data_to_silver_task