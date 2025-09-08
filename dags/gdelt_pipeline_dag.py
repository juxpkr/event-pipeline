from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import os
import pendulum

with DAG(
    dag_id="gdelt_pipeline_full",
    start_date=pendulum.datetime(2025, 8, 26, tz="Asia/Seoul"),
    schedule=None,  # 수동 트리거
    catchup=False,
    max_active_runs=1,  # 동시 실행 방지
    doc_md="""
    ### GDELT 전체 파이프라인 DAG
    - 목적: Raw Producer → Kafka → MinIO Raw Bucket → Spark Processor → MinIO Silver Table
    - 순서: Raw Producer → Kafka to MinIO → Silver Processor
    """,
) as dag:
    # 공통 상수 정의
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"

    # GDELT Raw 데이터 Producer (Kafka로 전송)
    task_raw_producer = BashOperator(
        task_id="gdelt_raw_producer",
        bash_command=f"PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/ingestion/gdelt/gdelt_raw_producer.py",
        env=dict(os.environ),
    )

    # Spark Processor (Kafka → MinIO Silver Table)
    task_silver_processor = BashOperator(
        task_id="gdelt_silver_processor",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"{PROJECT_ROOT}/src/processing/batch/gdelt_silver_processor.py"
        ),
        env=dict(os.environ),
    )

    # Task 순서 정의
    task_raw_producer >> task_silver_processor
