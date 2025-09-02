#!/bin/sh
set -e

echo "Initializing Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create --username airflow --firstname admin --lastname admin --role Admin --email admin@example.com --password airflow

echo "Airflow initialization completed!"