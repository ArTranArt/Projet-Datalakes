from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Définition du DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "datalake_pipeline",
    default_args=default_args,
    description="DAG pour exécuter le pipeline de traitement des données",
    schedule_interval=None,  # Modifie cette ligne si tu veux une exécution régulière (ex: "0 0 * * *")
    catchup=False,
)

# task_big_fetch = BashOperator(
#     task_id="run_big_fetch",
#     bash_command='python /opt/airflow/scripts/big_fetch.py --start-year 2018 --end-year 2018',
#     dag=dag
#     )

task_big_fetch = BashOperator(
    task_id="run_big_fetch",
    bash_command='python /opt/airflow/scripts/fetch.py --year 2023 --month 12',
    dag=dag
    )

task_unpack_to_raw = BashOperator(
    task_id="run_unpack_to_raw",
    bash_command='python /opt/airflow/scripts/unpack_to_raw.py --bucket_name raw',
    dag=dag,
)

task_preprocess_to_staging = BashOperator(
    task_id="run_preprocess_to_staging",
    bash_command='python /opt/airflow/scripts/preprocess_to_staging.py ',
    dag=dag,
)

task_process_to_curated = BashOperator(
    task_id="run_process_to_curated",
    bash_command='python /opt/airflow/scripts/process_to_curated.py ',
    dag=dag,
)

task_big_fetch >> task_unpack_to_raw >> task_preprocess_to_staging >> task_process_to_curated
# task_unpack_to_raw >> task_preprocess_to_staging >> task_process_to_curated
