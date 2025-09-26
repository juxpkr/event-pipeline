"""
GDELT Lifecycle Audit DAG
Event lifecycle 기반 데이터 감사 시스템
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


with DAG(
    dag_id="gdelt_lifecycle_audit",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    description="GDELT Event Lifecycle Audit System",
    schedule=None,  # 트리거 기반 실행으로 변경
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "audit", "monitoring"],
) as dag:

    # Lifecycle Consolidation Task
    consolidate_lifecycle = SparkSubmitOperator(
        task_id="consolidate_lifecycle_data",
        conn_id="spark_conn",
        packages="io.delta:delta-core_2.12:2.4.0",
        execution_timeout=timedelta(minutes=10),
        application="/opt/airflow/src/audit/lifecycle_consolidator.py",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
        doc_md="""
        Lifecycle Data Consolidation
        - Read from staging tables: lifecycle_staging_event, lifecycle_staging_gkg
        - Merge into main table: s3a://warehouse/audit/lifecycle
        - Clean up processed staging data
        - Maintain audit trail integrity
        """,
    )

    # Lifecycle Audit 실행
    lifecycle_audit_task = SparkSubmitOperator(
        task_id="run_lifecycle_audit",
        conn_id="spark_conn",
        packages="io.delta:delta-core_2.12:2.4.0",
        execution_timeout=timedelta(minutes=10),
        application="/opt/airflow/src/validation/lifecycle_auditor.py",
        application_args=["--hours-back", "15"],
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
        env_vars={
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "PROMETHEUS_PUSHGATEWAY_URL": "http://pushgateway:9091",
            "POSTGRES_JDBC_URL": "jdbc:postgresql://postgres:5432/airflow",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
        },
    )

    consolidate_lifecycle >> lifecycle_audit_task
