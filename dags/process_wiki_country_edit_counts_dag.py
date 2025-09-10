from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define the absolute path to the Spark job script inside the Airflow container.
SPARK_JOB_PATH = "/opt/airflow/src/processing/batch/process_wiki_country_edit_counts.py"
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

with DAG(
    dag_id="process_1min_wiki_country_edit_counts",
    start_date=pendulum.datetime(2024, 5, 24, tz="UTC"),
    schedule_interval="* * * * *",  # Run every 1 minute
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
    process_and_aggregate_task = SparkSubmitOperator(
        task_id="run_spark_job_aggregate_wiki_counts",
        application=SPARK_JOB_PATH,
        conn_id="spark_default",
        master=SPARK_MASTER_URL,
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        verbose=True,
    )
