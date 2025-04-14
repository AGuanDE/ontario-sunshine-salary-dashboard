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

3.  **The dbt models will run after.** After Airflow has finished ingesting and processing the data, it will trigger the 'trigger_dbt_runner' task which will spin up another docker container to run the dbt models (dbt runs in a separate container due to dependency conflicts with Airflow i.e. dbt-core).

    The primary dbt model (`models/staging/stg_salary_canon.sql`) processes the cleaned data and materializes it as a table in your data warehouse (e.g., BigQuery). To optimize query performance and reduce costs for downstream analysis (like in the Streamlit app), this table is explicitly configured with:
    * **Partitioning:** The table is partitioned by the `calendar_year` column 
    * **Clustering:** Within each partition, the data is clustered by the `job_title` and `sector` columns.

    Once the dbt models are successfully built with these optimizations, the final tables are ready for visualization.

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

# MORE INFORMATION...

## Understanding the Workflow & Scripts

This section provides more detail on the pipeline stages and the purpose of each script involved.

### Detailed Pipeline Stages (As Orchestrated by Airflow)

The `data_ingestion_dag` in Airflow orchestrates the following main steps for *each year* of data, typically processing from 1996 (beginning of sunshine list) up to the most recent available year:

1.  **Ingestion (`ingest_data.py`)**:
    * Reads the `scripts/url_config.yaml` file (which is found in the scripts folder) to find the official URLs for the main salary disclosure CSV and the addendum CSV for the target year.
    * Downloads these two files using the specified URLs.
    * Saves the downloaded files locally within the Airflow container, typically into paths like `data/raw/salary/salary_{year}_raw.csv` and `data/raw/addendum/addendum_{year}_raw.csv`. Note that not all years have an addendum file.

2.  **Upload Raw to GCS (`upload_raw_to_gcs.py`)**:
    * Scans the local `data/raw/salary` and `data/raw/addendum` directories.
    * Extracts the year from each filename.
    * Standardizes the filenames (e.g., `salary_1996_raw.csv` becomes `sunshine_salary_1996.csv`).
    * Uploads these standardized raw files to your Google Cloud Storage (GCS) bucket (specified by the `--bucket` argument, default `sunshine-list-bucket`) under the `raw/salary/` and `raw/addendum/` prefixes. This script requires Google Cloud credentials configured in the environment.

3.  **Merge Addendum (`merge_addendum_gcs.py` & `merge_addendum.py`)**:
    * The `_gcs` script orchestrates the process using GCS. It identifies the year to process.
    * It downloads the corresponding raw `sunshine_salary_{year}.csv` and `sunshine_addendum_{year}.csv` files from the `raw/` prefix in GCS to a temporary location.
    * It then calls the core logic in `merge_addendum.py`.
    * **Core Merge Logic (`merge_addendum.py`)**:
        * Reads both CSVs, attempting multiple character encodings (`utf-8`, `iso-8859-1`, etc.) to handle potential inconsistencies in the source files. It uses the efficient `pyarrow` engine for reading.
        * Standardizes column names based on predefined mappings (e.g., 'Salary Paid' -> 'salary_paid').
        * **Deduplicates** both the salary and addendum files individually *before* merging. If duplicate entries (same person, employer, job, year) exist with different salaries, it keeps the one with the higher total compensation (`salary_paid` + `taxable_benefits`).
        * Identifies the 'status' column in the addendum and normalizes its values to 'addition', 'deletion', or 'changed'.
        * Applies the addendum changes:
            * Rows marked 'deletion' are removed from the salary data.
            * Rows marked 'addition' are appended to the salary data.
            * Rows marked 'changed' are compared to existing salary data. If the addendum row is identical to an existing row, it's skipped. If it's different, the old row is removed, and the 'changed' row from the addendum is added.
        * Uses a composite key (`_match_key`) based on normalized name, employer, job title, and year to match records between files.
        * Performs a final deduplication step after all changes are applied.
        * Saves the result to a temporary file named `merged_salary_{year}_uncleaned.csv`.
    * `merge_addendum_gcs.py` uploads this temporary merged file back to GCS under the `merged/` prefix.

4.  **Validate Merge (`validate_merge_gcs.py` & `validate_merge.py`)**:
    * Similar to the merge step, the `_gcs` script orchestrates by downloading the merged file and the original raw salary/addendum files from GCS for a given year.
    * It calls the core logic in `validate_merge.py`.
    * **Core Validation Logic (`validate_merge.py`)**:
        * Checks if rows marked for 'deletion' in the addendum are truly absent in the final merged file.
        * Checks if rows marked for 'addition' were successfully added to the merged file.
        * Reports any discrepancies found. This acts as a sanity check on the merge process.

5.  **Clean Data (`clean_salary_data_gcs.py` & `clean_salary_data.py`)**:
    * The `_gcs` script downloads the `merged_salary_{year}_uncleaned.csv` file from the `merged/` prefix in GCS.
    * It calls the core logic in `clean_salary_data.py`.
    * **Core Cleaning Logic (`clean_salary_data.py`)**:
        * Reads the merged CSV, again trying multiple encodings.
        * Standardizes column names.
        * Performs extensive normalization:
            * Text fields (names, employer, job title, sector) are trimmed, and special characters/quotes are standardized.
            * Employer names have common abbreviations expanded (e.g., 'Univ.' -> 'University').
            * Job titles are normalized using a large mapping to standardize common titles (e.g., 'TEACHER, ELEMENTARY' -> 'Elementary Teacher').
            * First and last names are capitalized consistently.
        * Numeric fields (`salary_paid`, `taxable_benefits`) are cleaned by removing non-numeric characters (like '$', ',') and converted to float, filling errors/NaNs with 0.
        * Creates derived columns: `full_name` and `total_compensation`.
        * Ensures `calendar_year` is an integer type.
        * Performs a final deduplication (using the same salary resolution logic as the merge step).
        * Drops rows that have null or empty values in essential text columns (sector, names, employer, job title) after cleaning.
        * Saves the result to a temporary file `sunshine_cleaned_{year}.csv`, ensuring all non-numeric fields are quoted to handle potential commas within fields (like job titles).
    * `clean_salary_data_gcs.py` uploads this final cleaned file to GCS under the `cleaned/` prefix.

6.  **Validate Cleaning (`validate_cleaning_gcs.py`)**:
    * Downloads the cleaned file and the corresponding merged file from GCS.
    * Checks the cleaned file against a predefined schema:
        * Verifies that all required columns are present.
        * Ensures key columns do not contain null values.
        * Confirms that columns have the expected data types (e.g., `salary_paid` is float, `calendar_year` is integer).
    * Reports any schema violations found.

7.  **Trigger dbt Run**: After all years are processed and validated by the Airflow DAG, a final task triggers a separate Docker container (`dbt_runner`) to execute the dbt models.
    * dbt connects to the destination (e.g., BigQuery).
    * It reads the cleaned data (presumably from the `cleaned/` location in GCS, often loaded into staging tables in BigQuery).
    * It applies the transformations defined in your dbt project's models (e.g., creating views, joining data, calculating aggregates).
    * The final modeled data resides in BigQuery tables/views ready for analysis.

8.  **Visualization (Streamlit)**: The `streamlit_app.py` is run manually.
    * It connects to BigQuery (using credentials from `GOOGLE_APPLICATION_CREDENTIALS`).
    * It queries the final tables/views created by dbt.
    * It presents an interactive dashboard based on that data.

### Script folder Breakdown

* **`ingest_data.py`**: Downloads raw yearly data based on `url_config.yaml`.
* **`url_config.yaml`**: Stores the source URLs for salary and addendum files for each year.
* **`upload_raw_to_gcs.py`**: Takes local raw files, standardizes names, uploads to GCS `raw/` prefix.
* **`merge_addendum.py`**: Core logic to combine salary + addendum data, handling duplicates and status changes. (Local file I/O).
* **`merge_addendum_gcs.py`**: Orchestrates the merge using GCS for input/output, calling `merge_addendum.py` logic.
* **`clean_salary_data.py`**: Core logic for cleaning, normalizing, and standardizing data within a single merged file. (Local file I/O).
* **`clean_salary_data_gcs.py`**: Orchestrates cleaning using GCS input/output, calling `clean_salary_data.py` logic.
* **`validate_merge.py`**: Core logic to check if deletions/additions from addendum were applied correctly in the merged file. (Local file I/O).
* **`validate_merge_gcs.py`**: Orchestrates merge validation using GCS input/output, calling `validate_merge.py` logic.
* **`validate_cleaning_gcs.py`**: Checks the final cleaned data in GCS against schema expectations (columns, nulls, dtypes).
* **`gcs_modules.py`**: Utility functions for common GCS operations (download, upload, check existence, parse path) used by `_gcs.py` scripts.
* **`streamlit_app.py`**: (Not provided, but mentioned) Assumed to query final dbt models from BigQuery and display results.

### Key Concepts Explained

* **Addendum Files**: These files, provided alongside the main salary list for most years, contain corrections or updates. They list records that should be added, deleted, or changed in the main list. The `merge_addendum.py` script is crucial for applying these changes accurately.
* **Deduplication**: The source data occasionally contains duplicate entries for the same person/job/year, sometimes with different salary figures. The scripts perform deduplication at multiple stages (before merging, after merging, after cleaning). The strategy is to keep the entry with the highest `total_compensation` when resolving salary discrepancies for otherwise identical rows (we assume the higher salary is the truth). Exact duplicates are simply dropped.
* **Normalization**: Data comes in various formats (e.g., 'TEACHER', 'Teacher, Elementary', 'ENSEIGNANT'). The `clean_salary_data.py` script applies normalization rules to standardize text fields like job titles, employer names, and personal names for consistency. Numeric fields are also cleaned to remove currency symbols and commas.
* **Encoding Handling**: Source CSVs may use different character encodings. The `load_csv_with_encoding` function (used in `merge_addendum.py` and `clean_salary_data.py`) attempts several common encodings (`utf-8`, `iso-8859-1`, etc.) to ensure the files can be read correctly.
* **Validation**: Separate validation scripts (`validate_merge*.py`, `validate_cleaning_gcs.py`) are included to verify the integrity of the data after key transformation steps (merging and cleaning). They check for correctness (merge logic applied) and schema compliance (cleaning produced expected format).
* **Local vs. Cloud (`_gcs.py` scripts)**: The project provides pairs of scripts for core operations (merge, clean, validate). Scripts *without* `_gcs` in the name (`merge_addendum.py`, `clean_salary_data.py`, `validate_merge.py`) operate purely on local files specified via command-line arguments. Scripts *with* `_gcs` (`merge_addendum_gcs.py`, etc.) orchestrate the process using Google Cloud Storage for inputs and outputs, calling the core logic from the non-GCS scripts after downloading files to temporary local storage. The Airflow DAG is configured to use these `_gcs.py` scripts by default.

### Running Individual Scripts

While Airflow runs the end-to-end pipeline, you can run individual scripts manually for testing or debugging specific steps. Remember to provide the necessary command-line arguments.

* **Download Raw Data for a Year:**
    ```bash
    python scripts/ingest_data.py --year 2022 --config scripts/url_config.yaml
    ```
    *(Requires `url_config.yaml`. Saves to `data/raw/...`)*

* **Upload Raw Data to GCS (Specific Year):**
    ```bash
    python scripts/upload_raw_to_gcs.py --bucket your-gcs-bucket-name --year 2022
    ```
    *(Requires local files in `data/raw/...` and GCS credentials)*

* **Merge Local Files:**
    ```bash
    python scripts/merge_addendum.py --salary data/raw/salary/salary_2022_raw.csv --addendum data/raw/addendum/addendum_2022_raw.csv --output data/merged_local
    ```
    *(Outputs `merged_salary_2022_uncleaned.csv`)*

* **Merge Files from GCS (Specific Year):**
    ```bash
    python scripts/merge_addendum_gcs.py --bucket your-gcs-bucket-name --year 2022
    ```
    *(Requires raw files in GCS, GCS credentials. Outputs to `gs://your-gcs-bucket-name/merged/`)*

* **Clean Local Merged File:**
    ```bash
    python scripts/clean_salary_data.py --input data/merged_local/merged_salary_2022_uncleaned.csv --output-dir data/cleaned_local
    ```
    *(Outputs `sunshine_cleaned_2022.csv`)*

* **Clean Merged File from GCS (Specific Year):**
    ```bash
    python scripts/clean_salary_data_gcs.py --bucket your-gcs-bucket-name --year 2022
    ```
    *(Requires merged file in GCS, GCS credentials. Outputs to `gs://your-gcs-bucket-name/cleaned/`)*

* **Validate Local Merge:**
    ```bash
    python scripts/validate_merge.py --salary data/raw/salary/salary_2022_raw.csv --addendum data/raw/addendum/addendum_2022_raw.csv --merged data/merged_local/merged_salary_2022_uncleaned.csv
    ```
    *(Prints validation results)*

* **Validate Merged Files in GCS:**
    ```bash
    python scripts/validate_merge_gcs.py --bucket your-gcs-bucket-name
    ```
    *(Validates all merged files found in the bucket)*

* **Validate Cleaned Files in GCS (Specific Year):**
    ```bash
    python scripts/validate_cleaning_gcs.py --bucket your-gcs-bucket-name --year 2022
    ```
    *(Validates the cleaned file against schema expectations)*

### Configuration Details

* **`scripts/url_config.yaml`**: This file is essential for the data ingestion step. It maps each year (as a string) to the direct download URL for the corresponding salary CSV and addendum CSV. Ensure this file is present in the scripts folder.
* **GCS Bucket Name**: Most `_gcs.py` scripts take a `--bucket` argument, defaulting to `sunshine-list-bucket`. You'll need to either use this default name when creating your bucket (e.g., via Terraform) or pass your specific bucket name when running scripts or configuring Airflow connections/variables.
* **GCP Credentials**: Scripts interacting with GCS (`upload_raw_to_gcs.py`, `*_gcs.py`, `dbt` and `streamlit_app.py`) rely on Application Default Credentials (ADC). Ensure the `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to your service account key JSON file, or that credentials are otherwise configured correctly in the environment where these scripts run (like the Airflow Docker container).

### Potential Issues & Troubleshooting

* **Missing Addendum**: The scripts handle missing addendum files gracefully (e.g., `merge_addendum.py` just passes the salary file through). Validation steps also account for this.
* **GCS Permissions**: Ensure the service account used has the necessary permissions (`storage.objects.create`, `storage.objects.get`, `storage.objects.list`) on the target GCS bucket.
* **Airflow Task Failures**: Check the Airflow logs for the specific failed task. This often reveals issues like incorrect paths, missing credentials, script errors (e.g., a specific file causing a cleaning step to fail), or timeouts.