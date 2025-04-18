# ./airflow/dags/run_dbt.py
# this doesn't work unfortunately :(

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
import os

# Defines the path for the service account key *inside the dbt container*
# This path will be the target of the volume mount in DockerOperator
GCP_KEY_PATH_IN_DBT_CONTAINER = "/secrets/gcp_key.json"

default_args = {
    'owner': 'aguan',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='trigger_dbt_runner',
    default_args=default_args,
    start_date=datetime(2025, 4, 15),
    schedule=None,
    catchup=False,
) as dag:

    # Determine the host path to the dbt project directory relative to docker-compose.yml
    # Assumes docker-compose.yml is in the project root.

    host_dbt_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../sunshine_dbt"))

    host_gcp_key_path = os.path.abspath("ontario-sunshine-service-account.json")
    host_profiles_path = os.path.expanduser('~') + "/.dbt/profiles.yml"

    run_dbt = DockerOperator(
        task_id='run_dbt_docker_operator',
        image='ontario-sunshine-salary-dashboard-dbt-runner:latest', # Adjust if you tagged it differently
        api_version='auto',
        auto_remove='success',
        command="sh -c 'dbt deps && dbt seed && dbt run'",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source=host_dbt_project_path,
                target="/opt/airflow/sunshine_dbt",
                type="bind"
            ),
            Mount(
                source=host_gcp_key_path,
                target=GCP_KEY_PATH_IN_DBT_CONTAINER,
                type="bind",
                read_only=True
            ),
            Mount(
                source=host_profiles_path,
                target="/root/.dbt/profiles.yml",
                type="bind",
                read_only=True
            )
        ],
        environment={
            # Tell dbt where profiles.yml is within the container
            'DBT_PROFILES_DIR': '/root/.dbt',
            # Tell GCP libraries where the key is within the container
            'GOOGLE_APPLICATION_CREDENTIALS': GCP_KEY_PATH_IN_DBT_CONTAINER
        },
        working_dir='/opt/airflow/sunshine_dbt'
    )

    run_dbt

if __name__ == "__main__":
    dag.test()