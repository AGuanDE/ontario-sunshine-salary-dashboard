from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Updated import
from datetime import datetime

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    start >> end  # Add task dependency