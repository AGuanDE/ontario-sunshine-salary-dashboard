# docker/Dockerfile
FROM apache/airflow:2.10.5-python3.8

USER airflow
WORKDIR /opt/airflow

# Copy requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt
COPY . /opt/airflow

# Set Airflow and Python version dynamically for constraints
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.8

# Define the constraints URL
# constraint file ensures all packages will be compatible
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Only install airflow with constraints
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} --constraint "${CONSTRAINT_URL}"

# THEN install your packages (without constraint file!)
RUN pip install --no-cache-dir -r /tmp/requirements.txt