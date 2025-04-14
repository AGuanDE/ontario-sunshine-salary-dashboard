from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'aguan',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='trigger_dbt_runner_via_compose',
    default_args=default_args,
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_dbt_via_compose = BashOperator(
        task_id='run_dbt_via_compose',
        bash_command=(
            'cd /home/aguan/ontario-sunshine-salary-dashboard && '
            'docker-compose run --rm dbt-runner'
        ),
    )

    run_dbt_via_compose