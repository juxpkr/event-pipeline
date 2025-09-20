"""
GDELT End-to-End Pipeline DAG
GDELT 3개 데이터타입 수집 → Kafka → Bronze Layer → Silver Layer
"""

from __future__ import annotations
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import pendulum

with DAG(
    dag_id="gdelt_bronze_to_silver",
    start_date=pendulum.datetime(2025, 9, 17, tz="Asia/Seoul"),
    schedule="0,15,30,45 * * * *",  # 정각 기준 15분 단위 실행
    catchup=False,
    max_active_runs=1,
    doc_md="""
    GDELT End-to-End Pipeline
    - 목적: GDELT 데이터 수집 → Bronze Layer → Silver Layer 완전 파이프라인
    - 데이터: 최신 15분 배치 데이터 (Events, Mentions, GKG)
    - 결과: MinIO Silver Layer에 정제된 분석용 데이터 저장

    실행 순서:
    1. GDELT Producer → Kafka (3개 토픽: events, mentions, gkg)
    2. Bronze Consumer → MinIO Bronze Layer (Delta 형식)
    3. Silver Processor → MinIO Silver Layer (Events, Events Detailed)
    """,
) as dag:
    # 공통 상수
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"

    SPARK_CONN_ID = "spark_conn"

    # Task 1: GDELT 3-Way Producer → Kafka
    gdelt_producer = BashOperator(
        task_id="gdelt_producer",
        bash_command=f"PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/src/ingestion/gdelt_producer.py --logical-date '{{{{ data_interval_start }}}}'",
        execution_timeout=timedelta(minutes=10),
        env=dict(os.environ),
        doc_md="""
        GDELT 3-Way Producer
        - Events, Mentions, GKG 데이터를 각각 수집
        - 3개 Kafka 토픽에 분리 저장 (gdelt_events_bronze, gdelt_mentions_bronze, gdelt_gkg_bronze)
        - 최신 15분 배치 데이터 처리
        """,
    )

    # Task 2: Bronze Consumer → MinIO Bronze Layer
    bronze_consumer = SparkSubmitOperator(
        task_id="bronze_consumer",
        # Airflow UI에서 만들어야 할 Spark Master 접속 정보
        conn_id=SPARK_CONN_ID,
        application="/opt/airflow/src/ingestion/gdelt_bronze_consumer.py",
        conf={
            "spark.cores.max": "4",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "2",
        },
    )

    # Task 3: Silver Layer Processing
    silver_processor = SparkSubmitOperator(
        task_id="silver_processor",
        conn_id=SPARK_CONN_ID,
        application="/opt/airflow/src/processing/gdelt_silver_processor.py",
        # Airflow의 작업 시간 구간을 Spark 코드의 인자로 전달
        application_args=["{{ data_interval_start }}", "{{ data_interval_end }}"],
        conf={
            # Worker VM 2대에 각각 하나씩 Executor를 배치
            "spark.executor.instances": "2",
            # Worker의 8개 코어 중 6개를 Executor에 할당
            "spark.executor.cores": "6",
            # Worker의 32GB 메모리 중 18GB를 Executor에 할당
            "spark.executor.memory": "18g",
            # Driver는 보통 Manager 노드에서 실행되므로 적절한 메모리를 할당
            "spark.driver.memory": "4g",
            # 총 Executor 코어 수 (2 instances * 6 cores)와 일치시켜 Spark가 자원을 정확히 예측하도록 설정
            "spark.cores.max": "12",
        },
        doc_md="""
        Silver Layer Processing
        - Bronze Layer → Silver Layer 데이터 변환
        - 3-Way 조인 (Events + Mentions + GKG)
        - Delta Lake 파티션 저장 (default.gdelt_events, default.gdelt_events_detailed)
        """,
    )

    # Silver 작업이 성공하면, dbt DAG을 호출
    trigger_dbt_gold_pipeline = TriggerDagRunOperator(
        task_id="trigger_dbt_gold_pipeline",
        trigger_dag_id="gdelt_silver_to_gold",  # 방금 만든 새 DAG의 id
        wait_for_completion=False,  # 일단 호출만 하고 나는 내 할 일 끝냄
    )

    # Task 의존성 정의: Producer → Bronze → Silver → dbt
    gdelt_producer >> bronze_consumer >> silver_processor >> trigger_dbt_gold_pipeline
