"""
GDELT Bronze Consumer Events Test DAG
Single data_type (events) processing test DAG
"""

from __future__ import annotations
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import pendulum

with DAG(
    dag_id="gdelt_bronze_consumer_events_test",
    start_date=pendulum.datetime(2025, 9, 26, tz="Asia/Seoul"),
    schedule="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    max_active_runs=1,
    doc_md="""
    GDELT Bronze Consumer Events Test
    - Purpose: Test independent processing of events data type for lifecycle tracking
    - Scope: Kafka events topic to MinIO Bronze Layer
    - Test: Verify WAITING status is recorded in lifecycle
    """,
) as dag:
    # Common constants
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"
    SPARK_CONN_ID = "spark_conn"

    # Bronze Consumer for Events Only
    bronze_consumer_events = SparkSubmitOperator(
        task_id="bronze_consumer_events",
        conn_id=SPARK_CONN_ID,
        packages="io.delta:delta-core_2.12:2.4.0",
        application="/opt/airflow/src/ingestion/gdelt_bronze_consumer.py",
        application_args=["events"],  # Pass data_type as argument
        conf={
            "spark.cores.max": "2",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "1",
        },
        execution_timeout=timedelta(minutes=10),
        doc_md="""
        Events Bronze Consumer Test
        - Read data from Kafka events topic
        - Store to MinIO Bronze Layer in Delta format
        - Record WAITING status in lifecycle
        """,
    )

    bronze_consumer_events