import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

BASE_DIR = "/workspaces/hm2/Files/"

JOB1_PORT = 8081
JOB2_PORT = 8082

RAW_DIR = os.path.join(BASE_DIR, "raw", "sales")
STG_DIR = os.path.join(BASE_DIR, "stg", "sales")

def run_job1(date, **kwargs):
    raw_data_path = kwargs.get('raw_data_path')
    raw_folder_name = date.strftime('%Y-%m-%d')
    raw_folder_path = os.path.join(raw_data_path, raw_folder_name)
    date_str = date.strftime('%Y-%m-%d')
    print("Starting job1:")
    
    http_hook = HttpHook(method='POST', http_conn_id='job1_api')
    response = http_hook.run(
        endpoint='/fetch_api_data',
        data=json.dumps({
            "date": date_str,
            "raw_dir": raw_folder_path
        }),
        headers={'Content-Type': 'application/json'}
    )
    
    assert response.status_code == 201
    print("job1 completed!")
  
def run_job2(date, **kwargs):
    raw_data_path = kwargs.get('raw_data_path')
    stg_data_path = kwargs.get('stg_data_path')
    raw_folder_name = date.strftime('%Y-%m-%d')
    raw_folder_path = os.path.join(raw_data_path, raw_folder_name)
    stg_folder_name = date.strftime('%Y-%m-%d')
    stg_folder_path = os.path.join(stg_data_path, stg_folder_name)
    
    print("Starting job2:")
    
    http_hook = HttpHook(method='POST', http_conn_id='job2_api')
    response = http_hook.run(
        endpoint='/convert',
        data=json.dumps({
            "raw_dir": raw_folder_path,
            "stg_dir": stg_folder_path
        }),
        headers={'Content-Type': 'application/json'}
    )
    
    assert response.status_code == 201
    print("job2 completed!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

date_str = default_args['start_date']

with DAG(
    dag_id="process_sales",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    tags=['HM_3'],
    catchup=True
) as dag:
    extract_data_from_api = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=run_job1,
        op_kwargs={'date': date_str, 'raw_data_path': RAW_DIR},
        provide_context=True
    )

    convert_to_avro = PythonOperator(
        task_id='convert_to_avro',
        python_callable=run_job2,
        op_kwargs={'date': date_str, 'raw_data_path': RAW_DIR, 'stg_data_path': STG_DIR},
        provide_context=True
    )

    extract_data_from_api >> convert_to_avro
