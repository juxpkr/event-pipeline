"""
GDELT Audit Consolidator DAG
Staging lifecycle tables (event/gkg) to main lifecycle table consolidation
"""

from __future__ import annotations
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

with DAG(
    dag_id="gdelt_audit_consolidator",
    start_date=pendulum.datetime(2025, 9, 26, tz="Asia/Seoul"),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    doc_md="""
    GDELT Audit Consolidator
    - Purpose: Merge staging lifecycle tables into main lifecycle table
    - Scope: lifecycle_staging_event + lifecycle_staging_gkg → main lifecycle
    - Schedule: Every 5 minutes to maintain data freshness
    - Benefits: Avoid concurrency conflicts while maintaining unified audit view
    """,
    tags=["gdelt", "audit", "consolidation"],
) as dag:
    # Common constants
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"
    SPARK_CONN_ID = "spark_conn"

    # Lifecycle Consolidation Task
    consolidate_lifecycle = SparkSubmitOperator(
        task_id="consolidate_lifecycle_data",
        conn_id=SPARK_CONN_ID,
        packages="io.delta:delta-core_2.12:2.4.0",
        application="/opt/airflow/src/audit/lifecycle_consolidator.py",
        conf={
            "spark.cores.max": "2",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "1",
            "spark.driver.memory": "2g",
        },
        execution_timeout=timedelta(minutes=5),
        doc_md="""
        Lifecycle Data Consolidation
        - Read from staging tables: lifecycle_staging_event, lifecycle_staging_gkg
        - Merge into main table: s3a://warehouse/audit/lifecycle
        - Clean up processed staging data
        - Maintain audit trail integrity
        """,
    )

    consolidate_lifecycle