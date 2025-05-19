# Base image
FROM python:3.10-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install system dependencies
# RUN apt-get update && apt-get install -y build-essential libpq-dev curl && apt-get clean

# Upgrade pip and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Install Airflow and providers compatible with version 2.8.1
RUN pip install apache-airflow==2.8.1 \
    "apache-airflow[postgres]==2.8.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

# RUN pip install apache-airflow-providers-smtp==1.5.0
# RUN pip install apache-airflow-providers-postgres==5.7.0

# Copy application code
COPY . /app/

# Set working directory
WORKDIR /opt/airflow
