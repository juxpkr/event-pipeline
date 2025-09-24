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
    schedule="18,33,48,3 * * * *",  # 15분마다 실행 (18분, 33분, 48분, 03분)
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "audit", "monitoring"],
) as dag:

    # Lifecycle Audit 실행
    lifecycle_audit_task = SparkSubmitOperator(
        task_id="run_lifecycle_audit",
        conn_id="spark_conn",
        application="/opt/airflow/src/validation/lifecycle_auditor.py",
        application_args=["--hours-back", "24"],
        conf={
            "spark.cores.max": "2",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "1",
        },
        env_vars={
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "PROMETHEUS_PUSHGATEWAY_URL": "http://pushgateway:9091",
        },
    )

    # 올드 이벤트 자동 만료
    expire_old_events_task = SparkSubmitOperator(
        task_id="expire_old_events",
        conn_id="spark_conn",
        application="/opt/airflow/src/audit/expire_lifecycle_events.py",
        conf={
            "spark.cores.max": "2",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "1",
        },
        env_vars={
            "SPARK_MASTER_URL": "spark://spark-master:7077",
        },
    )

    # 태스크 의존성
    lifecycle_audit_task >> expire_old_events_task
