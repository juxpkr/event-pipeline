"""
GDELT 백필 Pipeline DAG
기존 Producer → Consumer → Processor 파이프라인을 활용한 과거 데이터 백필
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
import os
import pendulum

# 백필 설정
BACKFILL_START_DATE = datetime(2025, 9, 26, 0, 0)
BACKFILL_END_DATE = datetime(2025, 9, 26, 1, 0)


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
    max_active_tasks=4,  # 메모리 절약을 위해 동시 실행 제한
    doc_md=f"""
    GDELT 백필 Pipeline (TaskGroup 병렬 최적화)
    - 과거 GDELT 데이터 백필 ({BACKFILL_START_DATE.strftime("%Y-%m-%d")} ~ {BACKFILL_END_DATE.strftime("%Y-%m-%d")})
    - 방식: 기존 Producer → Consumer → Processor 병렬실행
    """,
) as dag:

    # 공통 상수
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"
    SPARK_CONN_ID = "spark_conn"

    # 15분 간격 시간 리스트 생성
    timestamps_to_process = generate_15min_list()

    # 각 단계별 Task 리스트 초기화
    producer_tasks = []
    consumer_tasks = []
    processor_tasks = []

    for i, timestamp_start_dt in enumerate(timestamps_to_process):
        # 다음 15분 계산
        start_time_obj = datetime.fromisoformat(timestamp_start_dt)
        end_time_obj = start_time_obj + timedelta(minutes=15)

        # batch_id를 안전하게 만들기
        batch_id = start_time_obj.strftime("%Y%m%d_%H%M%S")

        # Task 1: Producer → Kafka
        producer_task = BashOperator(
            task_id=f"gdelt_backfill_producer_{batch_id}",
            pool="spark_pool",
            bash_command=f"""
            PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/ingestion/gdelt_backfill_producer.py \
                --logical-date '{{{{ data_interval_start }}}}' \
                --backfill-start '{start_time_obj.isoformat()}' \
                --backfill-end '{end_time_obj.isoformat()}'
            """,
            execution_timeout=timedelta(minutes=20),
            env=dict(os.environ),
            doc_md=f"""
            GDELT 백필 Producer ({start_time_obj})
            - {start_time_obj} ~ {end_time_obj} (15분 배치) 데이터 수집
            - 3개 Kafka 토픽에 분리 저장
            """,
        )
        producer_tasks.append(producer_task)

        # Task 2: Bronze Consumer
        consumer_task = SparkSubmitOperator(
            task_id=f"gdelt_bronze_consumer_{batch_id}",
            pool="spark_pool",
            conn_id=SPARK_CONN_ID,
            packages="io.delta:delta-core_2.12:2.4.0",
            application=f"{PROJECT_ROOT}/src/ingestion/gdelt_bronze_consumer.py",
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
            Bronze Consumer ({start_time_obj})
            - Kafka에서 {start_time_obj} 데이터 읽어서 Bronze Layer 저장
            """,
        )
        consumer_tasks.append(consumer_task)

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
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
            },
            execution_timeout=timedelta(minutes=30),
            doc_md=f"""
            Silver Processor ({start_time_obj})
            - Bronze Layer에서 {start_time_obj} 데이터 읽어서 Silver Layer 변환
            """,
        )
        processor_tasks.append(processor_task)

    producer_tasks >> consumer_tasks >> processor_tasks
