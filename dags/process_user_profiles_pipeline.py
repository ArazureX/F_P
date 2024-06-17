from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_json_to_gbq():
    client = bigquery.Client()
    dataset_id = 'silver'
    table_id = 'user_profiles'

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True  
    )

    gcs_uri = 'gs://final_project_ex/user_profiles/user_profiles.json'


    table_ref = client.dataset(dataset_id).table(table_id)

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()  

    print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}.")

with DAG(
    'json_to_gbq_dag',
    default_args=default_args,
    description='A DAG to load NEWLINE DELIMITED JSON data from GCS to BigQuery',
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    load_to_gbq_task = PythonOperator(
        task_id='load_to_gbq_task',
        python_callable=load_json_to_gbq,
    )

    load_to_gbq_task
