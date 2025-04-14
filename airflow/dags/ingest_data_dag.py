from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'aguan',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='data_ingestion_dag',
    default_args=default_args,
    start_date=datetime(1996, 1, 1),
    schedule_interval='@yearly',
    catchup=True,
    max_active_runs=16,
) as dag:

    ingest = BashOperator(
        task_id='ingest_data_task',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/ingest_data.py '
            '--year {{ execution_date.year }}'
        ),
    )

    upload = BashOperator(
        task_id='upload_raw_to_gcs_task',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/upload_raw_to_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    merge = BashOperator(
        task_id='merge_raw_files_task',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/merge_addendum_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    validate_merge = BashOperator(
        task_id='validate_merge_task',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/validate_merge_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    clean = BashOperator(
        task_id='clean_data_task',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/clean_salary_data_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    validate_clean = BashOperator(
        task_id='validate_clean_task',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/validate_cleaning_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    ingest >> upload >> merge >> validate_merge >> clean >> validate_clean

    # Trigger the dbt-runner DAG
    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_runner',
        trigger_dag_id='trigger_dbt_runner_via_compose',  # the downstream DAG
        wait_for_completion=False,  # fire-and-forget
    )

    # Chain it so it runs after validate_clean
    validate_clean >> trigger_dbt