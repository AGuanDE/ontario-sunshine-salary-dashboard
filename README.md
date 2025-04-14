# Ontario Sunshine List Data Pipeline

## Introduction

This project automates the end-to-end workflow of **ingesting, cleaning, modeling, and visualizing** the Ontario Sunshine List data. It leverages modern data tooling to orchestrate the pipeline in a clear and reproducible way. Key technologies used include **Apache Airflow** for workflow orchestration, **dbt (Data Build Tool)** for data modeling, **Docker** for containerization, and **Terraform** (optional) for provisioning cloud infrastructure on Google Cloud Platform (GCP). A simple **Streamlit** app is provided to visualize the results.

The Ontario *Sunshine List* is an annual disclosure of public sector employees in Ontario earning $100,000 or more. This project is designed for learning purposes and demonstrates how multiple tools can work together in a data engineering pipeline.

## Problem Statement
Every year, the Ontario government publishes the “Sunshine List,” detailing public-sector employees earning over \$100,000. Manually downloading, combining, cleaning, and analyzing these large CSV files (one per year) is time‑consuming and error‑prone. The visualizations provided by the Ontario government are also quite basic and don't offer some key insights which job seekers may be interested in. This project automates that entire workflow—ingestion, cleaning, modeling, and visualization (which you can customize in the streamlit_app.py) — so you can focus on insights rather than boilerplate.

## **Project components:**

- **Apache Airflow:** Orchestrates the pipeline tasks (downloading data, uploading to cloud storage, merging files, cleaning data, etc.) in a defined order.
- **dbt:** Handles data transformations and modeling after raw data is ingested.
- **Streamlit:** Runs a simple web application to visualize or explore the final data (after Airflow and dbt have finished).
- **Docker:** Ensures all the above tools run in a contained environment
- **Terraform (optional):** Automates the creation of GCP resources like a BigQuery dataset and Cloud Storage bucket for those who want to deploy the pipeline in the cloud.

## Prerequisites

Before you begin, make sure you have the following:

- **Docker** (with Docker Compose) installed on your system. This is required to run Airflow and other components in containers.
- **Google Cloud account (optional):** If you plan to use the cloud workflow, you should have a GCP project and a service account JSON key with access to BigQuery and Cloud Storage.
- **Service Account Credentials (for GCP, optional):** Place your GCP service account JSON key file in the project directory. Rename it to **`ontario-sunshine-service-account.json`** (the Docker Compose file expects this name). This will be mounted into the Airflow container for authentication. For local runs of dbt or Streamlit, you should also set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to this file (so that Google APIs can locate your credentials).
- **Terraform installed (optional):** If you want to provision cloud infrastructure using Terraform, ensure you have Terraform installed and configured (and have credentials to deploy to your GCP project).

> **Note:** If you are on Linux, Docker may require an environment file for Airflow. Create an `.env` file in the project root with your user ID to avoid permission issues. For example, run:  
> ```bash
> echo -e "AIRFLOW_UID=$(id -u)" > .env 
> ``` 
> This ensures that files created by Airflow containers are owned by your user and not root ([Running Airflow in Docker — Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.5.0/howto/docker-compose/index.html#:~:text=On%20Linux%2C%20the%20quick,compose)) ([Running Airflow in Docker — Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.5.0/howto/docker-compose/index.html#:~:text=For%20other%20operating%20systems%2C%20you,get%20rid%20of%20the%20warning)).

## Running the Project Locally (Docker Compose)

Follow these steps to set up and run the data pipeline on your local machine using Docker:

1. **Clone the repository.** Clone the project repository to your local machine and navigate into it.  
   ```bash
   git clone https://github.com/<your-username>/ontario-sunshine-salary-dashboard.git  
   cd ontario-sunshine-salary-dashboard
   ```  
   All subsequent commands should be run from the project’s root directory.

2. **Start the Airflow ingestion pipeline.** We will use Docker Compose to set up Airflow (which includes a scheduler, web server, and a PostgreSQL metadata database). First, run the Airflow initialization service (this sets up the Airflow metadata database and creates an admin user):  
   ```bash
   docker compose up airflow-init
   ```  
   This one-time setup will migrate the Airflow database and create the default admin account. You should see a message like *"Admin user airflow created"* when it's done. If you need to create a user manually, you can run:
   
   ```bash
   airflow users create \
    --username user \
    --firstname first_name \
    --lastname last_name \
    --role Admin \
    --email email@example.com \
    --password admin
    ```
   
   Next, launch the Airflow scheduler and webserver (in detached mode so it runs in the background):  
   ```bash
   docker compose up -d
   ```  
   This command starts all Airflow services (web server on port 8080, scheduler, and database). Give it a few moments to fully start. You can check container status with `docker compose ps` or `docker ps` to ensure they are healthy. 

   Once Airflow is running, open your browser to **`http://localhost:8080`** to access the Airflow UI. Log in with the default credentials **username:** `airflow` / **password:** `airflow` (these were created during the init step ([Running Airflow in Docker — Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.5.0/howto/docker-compose/index.html#:~:text=The%20account%20created%20has%20the,airflow))). In the Airflow UI, you should see a DAG called **`data_ingestion_dag`** (it may be in a paused state initially). 

   **Trigger the DAG:** Turn on (unpause) the **`data_ingestion_dag`** by toggling the switch next to it, and then trigger a run (you can click the play button ► or simply unpausing may trigger it if catchup is enabled). This DAG will now start executing the pipeline tasks for each year of data from 1996 onward. The tasks include: downloading the Sunshine List salary data and addendum data (outlines additions, deletions and changes that need to be made to the salary data)for the year, uploading the raw data file to Google Cloud Storage (GCS), merging (which just changes the salary data based on the addendum file), validating the merge, cleaning the combined dataset, and validating the cleaned data.

   🕒 **Wait for completion:** The full pipeline (1996 up to the latest year) will take a few minutes to complete, since it is processing multiple years of data. You can monitor progress in the Airflow UI’s **Graph** or **Tree** view. Each year’s run will execute the chain of tasks one after another. Wait until all tasks across all years show a success status (green) before moving to the next step. The DAG is designed with catch-up enabled, so it will automatically run for each year up to the current year.

3. **The dbt models will run after.** After Airflow has finished ingesting and cleaning the data, it will trigger the 'trigger_dbt_runner' task which will spin up another docker container to run the dbt models (the dbt has to run in a separate container because of dependency conflicts with airflow). Once the dbt models are succesfully built, the tables are ready to be visualized. 

4. **Launch the Streamlit app.** Finally, you can run the Streamlit app to visualize the data. The Streamlit application will query the processed data (from BigQuery) and provide an interactive dashboard for the Sunshine List. To start the app, run:  
   ```bash
   streamlit run streamlit_app.py
   ```  
   *(This command should be run in the project directory or wherever the Streamlit_app.py script is located.)* 

   After running the above command, Streamlit will print a URL (by default, **http://localhost:8501**) in the terminal. Open that URL in your web browser to access the app. You should see the Sunshine List dashboard interface.

   **Important:** If the Streamlit app needs to access BigQuery to fetch data, it will require credentials. Ensure that your environment has the `GOOGLE_APPLICATION_CREDENTIALS` pointing to the service account key (just like for dbt). Streamlit will then be able to use Google’s client libraries to query BigQuery using those credentials. Once the app is running, you can interact with the data and verify that everything was set up correctly.

   When you're done, you can stop the Streamlit app by pressing **Ctrl+C** in the terminal. You can also bring down the Airflow Docker containers with `docker compose down` if you no longer need them running.


**Local vs. Cloud execution:** The Airflow DAG by default uses the cloud-based scripts (those ending in `_gcs.py`) for merging and cleaning data using GC ([ingest_data_dag.py](file://file-W9FAEtXg8tzDS4QVLqk9qj#:~:text=merge%20%3D%20BashOperator,bucket%27%20%29%2C))】. If you are running everything locally without GCP, the project also provides equivalent scripts for offline use (e.g., `merge_addendum.py` instead of `merge_addendum_gcs.py`). These local versions work with files on your local filesystem instead of cloud storage. In an entirely offline scenario, you could modify the Airflow DAG to use those local scripts, or run them manually in sequence, to achieve the same data processing without Google Cloud.

## Conclusion

By following the above steps, you have set up a complete data pipeline for the Ontario Sunshine List. 🎉 You used Docker to run Airflow (for ingestion and cleaning), dbt for transformations, and Streamlit for visualization. Optionally, you leveraged Terraform and GCP to deploy the infrastructure on the cloud. This project should give you hands-on experience with orchestrating a multi-step workflow and using cloud data warehouse tools. Feel free to explore the Airflow DAG, the dbt models, and the Streamlit app code and play around with it. Good luck!

