import os
import tempfile
from airflow import models
from airflow.operators.python_operator import PythonOperator  # Ensure PythonOperator is imported
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
import pandas as pd
from dateutil.parser import parse
from google.cloud import bigquery

GCS_BUCKET = 'final_project_ex'
GCS_PREFIX = 'sales/'
BRONZE_DATASET = 'bronze'
BRONZE_TABLE = 'sales'
SILVER_DATASET = 'silver'
SILVER_TABLE = 'sales'

BRONZE_TABLE_SCHEMA = [
    {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
]

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def load_csvs_to_bronze(**kwargs):
    gcs_hook = GCSHook()
    client = bigquery.Client()

    files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=GCS_PREFIX)

    csv_files = [file for file in files if file.endswith('.csv')]

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        for i, file in enumerate(csv_files):
            
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

    query = """
    CREATE OR REPLACE TABLE `silver.sales` 
    PARTITION BY purchase_date AS
    SELECT
        CAST(CustomerId AS STRING) AS client_id,
        CASE
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\d{2}-\d{2}$') THEN CAST(PurchaseDate AS DATE FORMAT 'YYYY-MM-DD')
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\w{3}-\d{2}$') THEN DATE(PARSE_DATE('%Y-%b-%d', PurchaseDate))
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}/\d{2}/\d{2}$') THEN DATE(PARSE_DATE('%Y/%m/%d', PurchaseDate))
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}/\w{3}/\d{2}$') THEN DATE(PARSE_DATE('%Y/%b/%d', PurchaseDate))
            ELSE NULL
        END AS purchase_date,
        Product AS product_name,
        CAST(REGEXP_REPLACE(Price, r'[^0-9.,]', '') AS FLOAT64) AS price
    FROM `bronze.sales`;
    """

    query_job = client.query(query)
    query_job.result()

with models.DAG(
    'process_sales_pipeline',
    default_args=default_args,
) as dag:

    load_csvs = PythonOperator(
        task_id='load_csvs_to_bronze',
        python_callable=load_csvs_to_bronze,
        provide_context=True,
    )

    transform_data = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver,
        provide_context=True,
    )

    load_csvs >> transform_data
