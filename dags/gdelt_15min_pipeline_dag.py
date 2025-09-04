from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import os
import pendulum

with DAG(
    dag_id="gdelt_15min_pipeline",
    start_date=pendulum.datetime(2025, 8, 26, tz="Asia/Seoul"),
    schedule=None,  # 테스트용 임시 수동 트리거. 실제로는 15분 간격으로 실행되어야 함
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
    gdelt_15min_to_kafka = BashOperator(
        task_id="gdelt_15min_to_kafka",
        bash_command=f"PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/ingestion/gdelt/gdelt_15min_to_kafka.py",
        env=dict(os.environ),
    )

    # Kafka to MinIO (Raw 데이터 저장)
    task_kafka_to_minio = BashOperator(
        task_id="kafka_raw_to_minio",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"{PROJECT_ROOT}/src/ingestion/gdelt/kafka_15min_raw_to_minio_consumer.py"
        ),
        env=dict(os.environ),
    )

    # Spark Processor (Kafka → MinIO Silver Table)
    gdelt_15min_to_silver = BashOperator(
        task_id="gdelt_15min_to_silver",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"{PROJECT_ROOT}/src/processing/batch/gdelt_15min_to_silver.py"
        ),
        env=dict(os.environ),
    )

    # Task 순서 정의
    gdelt_15min_to_kafka >> task_kafka_to_minio >> gdelt_15min_to_silver
