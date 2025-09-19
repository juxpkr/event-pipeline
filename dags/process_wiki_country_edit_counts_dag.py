from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the absolute path to the Spark job script inside the Airflow container.
SPARK_JOB_PATH = "/opt/airflow/src/processing/batch/process_wiki_country_edit_counts.py"
SPARK_MASTER_URL = "spark://spark-master:7077"

with DAG(
    dag_id="process_5min_wiki_country_edit_counts",
    start_date=pendulum.datetime(2024, 5, 24, tz="UTC"),
    # schedule_interval="* * * * *",  # Run every 1 minute
    schedule_interval="*/5 * * * *",  # test 5 minutes
    catchup=False,
    doc_md="""
    ### Wikipedia Country Edit Counts DAG (1-Minute)

    This DAG runs a Spark batch job every minute to:
    1. Read filtered Wikipedia edit events from the 'wiki_edits' Kafka topic.
    2. Aggregate the number of edits for each 3-letter country code.
    3. Append the results to the 'wiki_country_edit_counts_1min' Delta table in MinIO.
    """,
    tags=["wikipedia", "processing", "kafka", "spark", "aggregation", "1min"],
) as dag:
    process_and_aggregate_task = BashOperator(
        task_id="run_spark_job_aggregate_wiki_counts",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER_URL} "
            f"--deploy-mode client "
            f"{SPARK_JOB_PATH}"
        ),
        env=dict(os.environ),
    )
