from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'enrich_user_profiles_dag',
    default_args=default_args,
    description='A DAG to enrich user profiles and store in gold dataset',
    start_date=days_ago(1),
) as dag:

    enrich_user_profiles_task = BigQueryInsertJobOperator(
        task_id='enrich_user_profiles_task',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `lofty-stack-426117-q8.gold.user_profiles_enriched` AS
                    SELECT
                        CAST(c.client_id AS INT64) AS client_id,
                        COALESCE(SPLIT(p.full_name, ' ')[SAFE_OFFSET(0)], c.first_name) AS first_name,
                        COALESCE(SPLIT(p.full_name, ' ')[SAFE_OFFSET(1)], c.last_name) AS last_name,
                        COALESCE(p.state, c.state) AS state,
                        c.email,
                        c.registration_date,
                        p.birth_date,
                        CAST(p.phone_number AS STRING) AS phone_number
                    FROM
                        `lofty-stack-426117-q8.silver.customers` c
                    LEFT JOIN
                        `lofty-stack-426117-q8.silver.user_profiles` p
                    ON
                        c.email = p.email
                """,
                "useLegacySql": False,
            }
        },
        
    )

    enrich_user_profiles_task
