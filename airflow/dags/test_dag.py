from airflow import DAG
from airflow.operators.dummy import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    start = EmptyOperator(task_id="start")