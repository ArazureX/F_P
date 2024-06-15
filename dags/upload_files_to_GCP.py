from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import os

# Конфігурації
BASE_DIR = "/opt/airflow/files/raw/sales/"
GCS_BUCKET = "hm10_test_bucket"
GCS_ROOT_PATH = "src1/sales/v1/"

def list_files(**kwargs):
    execution_date = kwargs['execution_date']
    local_path = os.path.join(BASE_DIR, execution_date.strftime("%Y-%m-%d"))
    files = os.listdir(local_path)
    files = [os.path.join(local_path, file) for file in files if os.path.isfile(os.path.join(local_path, file))]
    kwargs['ti'].xcom_push(key='file_list', value=files)

def upload_files_to_gcs(**kwargs):
    
    ti = kwargs['ti']
    gcs_path = kwargs['gcs_path']
    files = ti.xcom_pull(task_ids='list_files', key='file_list')

    for file in files:
        file_name = os.path.basename(file)
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{file_name}',
            src=file,
            dst=os.path.join(gcs_path, file_name),
            bucket=GCS_BUCKET,
        )
        upload_task.execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='upload_files_to_gcs',
    default_args=default_args,
    start_date=datetime(2022, 8, 10),
    end_date=datetime(2022, 8, 11),
   # schedule_interval=timedelta(days=1),
    catchup=True,  # Щоб не виконувати для інших дат
    tags=['example'],
    
) as dag:

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
        provide_context=True,
    )

    upload_files_task = PythonOperator(
        task_id='upload_files_to_gcs',
        python_callable=upload_files_to_gcs,
        provide_context=True,
    )

    list_files_task >> upload_files_task
