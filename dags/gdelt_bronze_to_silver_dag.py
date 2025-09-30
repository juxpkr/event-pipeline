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
        conn_id=SPARK_CONN_ID,
        packages="io.delta:delta-core_2.12:2.4.0",
        application="/opt/airflow/src/ingestion/gdelt_bronze_consumer.py",
        env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
        conf={
            # Driver
            'spark.driver.memory': '8g',
            'spark.driver.cores': '2',

            # Executor 
            'spark.executor.instances': '6',
            'spark.executor.memory': '24g',
            'spark.executor.cores': '6',

            # Shuffle 및 메모리 관리 최적화. 불필요한 디스크 I/O 감소
            'spark.sql.shuffle.partitions': '50',
            'spark.default.parallelism': '72',

            # Memory 최적화
            'spark.memory.fraction': '0.8',
            'spark.executor.memoryOverhead': '4g',

            # AQE 활성화: 스파크가 스스로 최적화
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true', # 작은 파티션을 알아서 합쳐줌

            #"spark.executor.memory": "8g",
            #"spark.executor.cores": "2",
            #"spark.executor.instances": "5",
            #"spark.driver.memory": "4g",
        },
    )

    # Task 3: Lifecycle Consolidator (Staging → Main)
    consolidate_lifecycle = SparkSubmitOperator(
        task_id="consolidate_lifecycle",
        conn_id=SPARK_CONN_ID,
        packages="io.delta:delta-core_2.12:2.4.0",
        application="/opt/airflow/src/audit/lifecycle_consolidator.py",
        execution_timeout=timedelta(minutes=10),
        conf={
            # Driver
            'spark.driver.memory': '8g',
            'spark.driver.cores': '2',

            # Executor 
            'spark.executor.instances': '6',
            'spark.executor.memory': '24g',
            'spark.executor.cores': '6',

            # Shuffle 및 메모리 관리 최적화
            'spark.sql.shuffle.partitions': '72',
            'spark.default.parallelism': '72',

            # Memory 최적화
            'spark.memory.fraction': '0.8',
            'spark.executor.memoryOverhead': '4g',

            # AQE 활성화
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        },
        doc_md="""
        Lifecycle Consolidator
        - Staging 테이블 (lifecycle_staging_event, lifecycle_staging_gkg) 데이터를 Main lifecycle 테이블로 통합
        - WAITING 상태 이벤트를 Silver Processor가 읽을 수 있도록 준비
        - Staging 테이블 정리
        """,
    )

    # Task 4: Silver Layer Processing
    silver_processor = SparkSubmitOperator(
        task_id="silver_processor",
        conn_id=SPARK_CONN_ID,
        packages="io.delta:delta-core_2.12:2.4.0",
        application="/opt/airflow/src/processing/gdelt_silver_processor.py",
        env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
        # Airflow의 작업 시간 구간을 Spark 코드의 인자로 전달
        application_args=["{{ data_interval_start }}", "{{ data_interval_end }}"],
        conf={
            # Driver
            'spark.driver.memory': '8g',
            'spark.driver.cores': '2',

            # Executor
            'spark.executor.instances': '6',
            'spark.executor.memory': '24g',
            'spark.executor.cores': '6',

            # Shuffle 및 메모리 관리 최적화. 불필요한 디스크 I/O 감소
            'spark.sql.shuffle.partitions': '50',
            'spark.default.parallelism': '72',

            # Memory 최적화
            'spark.memory.fraction': '0.8',
            'spark.executor.memoryOverhead': '4g',

            # AQE 활성화: 스파크가 스스로 최적화
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',

            #"spark.executor.instances": "5",
            #"spark.executor.memory": "8g",
            #"spark.executor.cores": "2",
            #"spark.driver.memory": "4g",
            #"spark.sql.shuffle.partitions": "100",
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

    # Task 의존성 정의: Producer → Bronze → Consolidator → Silver → dbt
    gdelt_producer >> bronze_consumer >> consolidate_lifecycle >> silver_processor >> trigger_dbt_gold_pipeline
