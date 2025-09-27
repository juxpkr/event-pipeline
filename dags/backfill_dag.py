"""
GDELT 백필 DAG - 1단계 (Producer → Consumer → Silver)
특정 날짜 범위의 데이터를 Raw부터 Silver Layer까지 처리
"""

from __future__ import annotations
import os
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG 설정값
BACKFILL_START_DATE = "2023-08-01 00:00:00"
BACKFILL_END_DATE = "2023-09-01 23:59:59"

with DAG(
    dag_id='gdelt_backfill_stage1_to_silver',
    start_date=pendulum.now(tz="Asia/Seoul"),
    schedule=None,  # 수동 실행
    catchup=False,
    max_active_runs=1,
    tags=['backfill', 'gdelt', 'stage1'],
    doc_md=f"""
    # GDELT 백필 1단계: Producer → Consumer → Silver

    **처리 기간**: {BACKFILL_START_DATE} ~ {BACKFILL_END_DATE}

    ## 처리 단계
    1. **Producer**: GDELT 원본 데이터 수집 → Kafka
    2. **Consumer**: Kafka 스트림 데이터 → Bronze Layer (Delta)
    3. **Silver**: Bronze 데이터 정제 → Silver Layer (Delta)

    ## 특징
    - 백필 전용 스크립트 사용
    - 단계별 실패 시 재시작 가능
    - 시간 범위 기반 필터링
    """,
) as dag:

    # Task 1: GDELT Producer (Raw → Kafka)
    backfill_producer = PythonOperator(
        task_id='backfill_producer_to_kafka',
        python_callable=lambda: __import__('subprocess').run([
            'python',
            '/opt/airflow/src/processing/gdelt_producer_backfill.py',
            '--start-date', BACKFILL_START_DATE,
            '--end-date', BACKFILL_END_DATE,
            '--logical-date', BACKFILL_START_DATE
        ], check=True, cwd='/opt/airflow'),
        doc_md="""
        ### GDELT Producer Backfill
        - 지정된 기간의 GDELT 데이터를 GDELT 서버에서 다운로드
        - 체크포인트 무시하고 직접 날짜 범위 지정
        - Events, Mentions, GKG 3가지 데이터 타입 병렬 처리
        - Kafka 토픽별로 데이터 전송
        """
    )

    # Task 2: Bronze Consumer (Kafka → Bronze)
    backfill_consumer = SparkSubmitOperator(
        task_id='backfill_consumer_to_bronze',
        conn_id='spark_conn',
        application='/opt/airflow/src/processing/gdelt_bronze_consumer_backfill.py',
        application_args=[
            '--start-time', BACKFILL_START_DATE,
            '--end-time', BACKFILL_END_DATE
        ],
        packages="io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        conf={
            "spark.app.name": f"GDELT_Backfill_Consumer_{BACKFILL_START_DATE.replace(' ', '_').replace(':', '')}",
            "spark.executor.instances": "3",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "2g",
        },
        env_vars={
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6379",
            "PROJECT_ROOT": "/opt/airflow"
        },
        doc_md="""
        ### Bronze Consumer Backfill
        - Kafka에서 백필 시간 범위의 데이터만 읽기
        - .trigger(availableNow=True)로 배치 모드 실행
        - Bronze Layer에 Delta 형식으로 저장
        - 파티셔닝 및 MERGE를 통한 멱등성 보장
        """
    )

    # Task 3: Silver Processor (Bronze → Silver)
    backfill_silver = SparkSubmitOperator(
        task_id='backfill_processor_to_silver',
        conn_id='spark_conn',
        application='/opt/airflow/src/processing/gdelt_silver_processor.py',
        application_args=[BACKFILL_START_DATE, BACKFILL_END_DATE],
        packages="io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.app.name": f"GDELT_Backfill_Silver_{BACKFILL_START_DATE.replace(' ', '_').replace(':', '')}",
            "spark.executor.instances": "4",
            "spark.executor.memory": "6g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "3g",
        },
        env_vars={
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6379",
            "PROJECT_ROOT": "/opt/airflow"
        },
        doc_md="""
        ### Silver Processor
        - Bronze Layer에서 지정 시간 범위 데이터 읽기
        - Events, Mentions, GKG 3-Way 조인 수행
        - 데이터 정제 및 스키마 변환
        - Silver Layer에 분석용 테이블 저장
        """
    )

    # 작업 순서 정의
    backfill_producer >> backfill_consumer >> backfill_silver