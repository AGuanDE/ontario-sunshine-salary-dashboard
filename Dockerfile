# Dockerfile
FROM apache/airflow:2.10.5-python3.11

# Switch to root to install packages
USER root

# Change UID/GID here so that airflow has ownership to make directories
RUN usermod -u 1000 airflow && \
    chown -R airflow:root /opt/airflow /home/airflow /tmp /usr/local/lib/python3.11/site-packages || true

# Update package lists and install dependencies including Docker CLI which is needed
# for running the docker run commands for the run_dbt.py DAG
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg-agent \
        software-properties-common \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && add-apt-repository \
        "deb [arch=amd64] https://download.docker.com/linux/debian \
        $(lsb_release -cs) \
        stable" \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# switch back to airflow user (now UID 1000 GID 1000)
USER airflow
WORKDIR /opt/airflow

# Set the Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow

# Define the location of the airflow.cfg file:
ENV AIRFLOW_CONFIG=${AIRFLOW_HOME}/airflow_proj/airflow.cfg

# Copy requirements.txt into the image
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# Set Airflow and Python version dynamically for constraints
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

# Define the constraints URL
# constraint file ensures all packages will be compatible
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install airflow and requirements as the airflow user (UID 1000)
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} --constraint "${CONSTRAINT_URL}"
RUN pip install --no-cache-dir -r /tmp/requirements.txt