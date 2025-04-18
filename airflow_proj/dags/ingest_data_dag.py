# ./airflow/dags/ingest_data_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'aguan', # change to reflect yours
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

CURRENT_YEAR = datetime.now().year
FINAL_INGEST_YEAR = CURRENT_YEAR - 1  # e.g., if 2025, then final year is 2024


with DAG(
    dag_id='data_ingestion_dag',
    default_args=default_args,
    start_date=datetime(1996, 1, 1),
    schedule='@yearly',
    catchup=True,
    max_active_runs=16,
) as dag:

    ingest = BashOperator(
        task_id='ingest_data_task',
        bash_command=(
            'python /opt/airflow/scripts/ingest_data.py '
            '--year {{ execution_date.year }} '
            '--output-dir /opt/airflow/data/raw'
        ),
    )

    upload = BashOperator(
        task_id='upload_raw_to_gcs_task',
        bash_command=(
            'python /opt/airflow/scripts/upload_raw_to_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    merge = BashOperator(
        task_id='merge_raw_files_task',
        bash_command=(
            'python /opt/airflow/scripts/merge_addendum_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    validate_merge = BashOperator(
        task_id='validate_merge_task',
        bash_command=(
            'python /opt/airflow/scripts/validate_merge_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    clean = BashOperator(
        task_id='clean_data_task',
        bash_command=(
            'python /opt/airflow/scripts/clean_salary_data_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    validate_clean = BashOperator(
        task_id='validate_clean_task',
        bash_command=(
            'python /opt/airflow/scripts/validate_cleaning_gcs.py '
            '--bucket sunshine-list-bucket'
        ),
    )

    # Branching: Only trigger dbt after last year (e.g., 2024)
    def should_trigger_dbt(execution_date_str):
        exec_date = datetime.fromisoformat(execution_date_str)
        return 'trigger_dbt_runner' if exec_date.year == FINAL_INGEST_YEAR else 'skip_dbt_trigger'

    branch = BranchPythonOperator(
        task_id='check_if_final_year',
        python_callable=should_trigger_dbt,
        op_args=['{{ execution_date }}'],
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_runner',
        trigger_dag_id='trigger_dbt_runner',
        wait_for_completion=False,
    )

    skip_dbt_trigger = EmptyOperator(
        task_id='skip_dbt_trigger'
    )

    ingest >> upload >> merge >> validate_merge >> clean >> validate_clean
    validate_clean >> branch >> trigger_dbt
    validate_clean >> branch >> skip_dbt_trigger

if __name__ == "__main__":
    dag.test()