"""
GDELT 3-Way Test Pipeline DAG
테스트용 GDELT 3개 데이터타입 수집 → Silver 처리 → MinIO 저장
"""
from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import os
import pendulum

with DAG(
    dag_id="gdelt_3way_test_pipeline",
    start_date=pendulum.datetime(2025, 9, 15, tz="Asia/Seoul"),
    schedule="*/15 * * * *",  
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ### GDELT 3-Way Test Pipeline
    - 목적: GDELT Events, Mentions, GKG 데이터 수집 → Kafka → MinIO Silver 처리
    - 데이터: 최근 1시간 데이터 (3개 토픽 분리)
    - 결과: MinIO Silver Layer에 3-way join된 데이터 저장

    **실행 순서:**
    1. Raw Producer → Kafka (3개 토픽)
    2. Kafka Consumer → MinIO Raw (Parquet)
    3. Silver Processor → MinIO Silver (3-way join)
    """,
) as dag:
    # 공통 상수
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"

    # Task 1: GDELT 3-Way Raw Data 수집 → Kafka
    gdelt_3way_raw_producer = BashOperator(
        task_id="gdelt_3way_raw_producer",
        bash_command=f"PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/tests/gdelt_3way_raw_producer.py",
        env=dict(os.environ),
        doc_md="""
        **GDELT 3-Way Raw Producer**
        - Events, Mentions, GKG 데이터를 각각 수집
        - 3개 Kafka 토픽에 분리 저장
        - 최근 1시간 데이터 처리 (4개 파일)
        """,
    )

    # Task 2: GDELT 스키마 테스트
    test_gdelt_schemas = BashOperator(
        task_id="test_gdelt_schemas",
        bash_command=f"PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/tests/test_gdelt_schemas.py",
        env=dict(os.environ),
        doc_md="""
        **GDELT Schemas Test**
        - GDELT Events, Mentions, GKG 스키마 정의 확인
        - 스키마 유효성 검증
        """,
    )

    # Task 3: Silver Layer 처리 (3-way 모든 데이터셋)
    silver_processor = BashOperator(
        task_id="silver_processor",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"{PROJECT_ROOT}/src/tests/test_gdelt_3way_silver_processor.py"
        ),
        env=dict(os.environ),
        doc_md="""
        **3-Way Silver Layer Processor**
        - Kafka에서 3개 토픽 데이터 읽기 (Events, Mentions, GKG)
        - 각 데이터셋을 별도 Silver 테이블로 저장
        - 3개 테이블: gdelt_events_silver, gdelt_mentions_silver, gdelt_gkg_silver
        """,
    )

    # Task 의존성 정의
    gdelt_3way_raw_producer >> test_gdelt_schemas >> silver_processor