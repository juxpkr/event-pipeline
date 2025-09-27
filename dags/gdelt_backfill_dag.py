"""
GDELT 백필 Pipeline DAG
기존 Producer → Consumer → Processor 파이프라인을 활용한 과거 데이터 백필
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import pendulum

# 백필 설정
BACKFILL_START_DATE = datetime(2023, 8, 1)
BACKFILL_END_DATE = datetime(2023, 8, 2)  # 일단 1일만 테스트


def generate_15min_list():
    """백필할 시간 리스트 생성 (15분 단위 - GDELT 실제 배치 간격)"""
    timestamps = []
    current = BACKFILL_START_DATE
    while current < BACKFILL_END_DATE:
        timestamps.append(current.strftime("%Y-%m-%dT%H:%M:%S"))
        current += timedelta(minutes=15)  # 15분씩 증가
    return timestamps


with DAG(
    dag_id="gdelt_backfill_pipeline",
    start_date=pendulum.now(tz="Asia/Seoul"),
    schedule=None,  # 수동 실행
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,  # 동시 4개 시간 처리
    doc_md=f"""
    GDELT 백필 Pipeline
    - 목적: 과거 GDELT 데이터 백필 ({BACKFILL_START_DATE.strftime("%Y-%m-%d")} ~ {BACKFILL_END_DATE.strftime("%Y-%m-%d")})
    - 방식: 기존 Producer → Consumer → Processor 파이프라인 재활용

    실행 순서:
    1. GDELT Backfill Producer → Kafka (지정된 시간 데이터)
    2. Bronze Consumer → MinIO Bronze Layer (기존 로직)
    3. Silver Processor → MinIO Silver Layer (기존 로직)
    """,
) as dag:

    # 공통 상수
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"
    SPARK_CONN_ID = "spark_conn"

    # 15분 간격 시간 리스트 생성
    timestamps_to_process = generate_15min_list()

    for i, timestamp_start in enumerate(timestamps_to_process):
        # 다음 15분 계산
        timestamp_end = (
            datetime.fromisoformat(timestamp_start) + timedelta(minutes=15)
        ).strftime("%Y-%m-%dT%H:%M:%S")
        batch_id = timestamp_start.replace("-", "_").replace(":", "_")

        # Task 1: GDELT 백필 Producer → Kafka
        producer_task = BashOperator(
            task_id=f"gdelt_backfill_producer_{batch_id}",
            pool="spark_pool",
            bash_command=f"""
            PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/ingestion/gdelt_backfill_producer.py \
                --logical-date '{{{{ data_interval_start }}}}' \
                --backfill-start '{timestamp_start}' \
                --backfill-end '{timestamp_end}'
            """,
            execution_timeout=timedelta(minutes=20),
            env=dict(os.environ),
            doc_md=f"""
            GDELT 백필 Producer ({timestamp_start})
            - {timestamp_start} ~ {timestamp_end} (15분 배치) 데이터 수집
            - 3개 Kafka 토픽에 분리 저장
            """,
        )

        # Task 2: Bronze Consumer (기존 스크립트 재사용)
        consumer_task = SparkSubmitOperator(
            task_id=f"gdelt_bronze_consumer_{batch_id}",
            pool="spark_pool",
            conn_id=SPARK_CONN_ID,
            packages="io.delta:delta-core_2.12:2.4.0",
            application=f"{PROJECT_ROOT}/src/streaming/gdelt_bronze_consumer.py",
            application_args=["--logical-date", "{{ data_interval_start }}"],
            env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
            conf={
                "spark.cores.max": "4",
                "spark.executor.instances": "2",
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
            },
            execution_timeout=timedelta(minutes=20),
            doc_md=f"""
            Bronze Consumer ({timestamp_start})
            - Kafka에서 {timestamp_start} 데이터 읽어서 Bronze Layer 저장
            """,
        )

        # Task 3: Silver Processor (기존 스크립트 재사용)
        processor_task = SparkSubmitOperator(
            task_id=f"gdelt_silver_processor_{batch_id}",
            pool="spark_pool",
            conn_id=SPARK_CONN_ID,
            packages="io.delta:delta-core_2.12:2.4.0",
            application=f"{PROJECT_ROOT}/src/processing/gdelt_silver_processor.py",
            application_args=["--logical-date", "{{ data_interval_start }}"],
            env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
            conf={
                "spark.cores.max": "4",
                "spark.executor.instances": "2",
                "spark.executor.memory": "3g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
            },
            execution_timeout=timedelta(minutes=30),
            doc_md=f"""
            Silver Processor ({hour_start})
            - Bronze Layer에서 {hour_start} 데이터 읽어서 Silver Layer 변환
            """,
        )

        # 순서: Producer → Consumer → Processor
        producer_task >> consumer_task >> processor_task
