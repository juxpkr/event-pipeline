from __future__ import annotations
import os
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount

with DAG(
    dag_id="gdelt_silver_to_gold",
    start_date=pendulum.now(tz="Asia/Seoul"),
    schedule=None,  # 직접 실행하거나, 다른 DAG이 호출하도록 설정
    catchup=False,
    max_active_runs=1,
    doc_md="""
    Silver Layer 데이터를 dbt로 변환하여 Gold Layer를 만들고,
    최종 결과를 PostgreSQL 데이터 마트로 이전합니다.
    """,
) as dag:
    # 공통 상수
    SPARK_MASTER = "spark://spark-master:7077"
    # Airflow 워커 컨테이너 내부에 있는 dbt 프로젝트 경로를 변수로 지정
    dbt_project_host_path = f"{os.getenv('PROJECT_ROOT', '/opt/airflow')}/transforms"

    def mark_processing_complete(context):
        """태스크 성공을 기록하는 Python 함수"""
        from src.utils.spark_builder import get_spark_session
        from src.audit.lifecycle_updater import EventLifecycleUpdater

        task_id = context["task_instance_key_str"]
        print(f"Task {task_id} has completed.")

        spark = get_spark_session("Gold_Complete_Callback", "spark://spark-master:7077")
        try:
            lifecycle_updater = EventLifecycleUpdater(spark)

            # Silver 완료 상태인 이벤트들을 Gold 완료로 마킹
            lifecycle_df = spark.read.format("delta").load(
                lifecycle_updater.lifecycle_path
            )
            silver_complete_events = (
                lifecycle_df.filter(lifecycle_df["status"] == "SILVER_COMPLETE")
                .select("global_event_id")
                .rdd.map(lambda row: row[0])
                .collect()
            )

            if silver_complete_events:
                lifecycle_updater.mark_gold_processing_complete(
                    silver_complete_events, f"gold_{context['ds']}"
                )
                print(
                    f"Marked {len(silver_complete_events)} events as Gold processing complete"
                )
        finally:
            spark.stop()

    def mark_postgres_complete(context):
        """Postgres 마이그레이션 완료를 기록하는 Python 함수"""
        from src.utils.spark_builder import get_spark_session
        from src.audit.lifecycle_updater import EventLifecycleUpdater

        task_id = context["task_instance_key_str"]
        print(f"Task {task_id} has completed.")

        spark = get_spark_session(
            "Postgres_Complete_Callback", "spark://spark-master:7077"
        )
        try:
            lifecycle_updater = EventLifecycleUpdater(spark)

            # Gold 완료 상태인 이벤트들을 Postgres 완료로 마킹
            lifecycle_df = spark.read.format("delta").load(
                lifecycle_updater.lifecycle_path
            )
            gold_complete_events = (
                lifecycle_df.filter(lifecycle_df["status"] == "GOLD_COMPLETE")
                .select("global_event_id")
                .rdd.map(lambda row: row[0])
                .collect()
            )

            if gold_complete_events:
                lifecycle_updater.mark_postgres_migration_complete(
                    gold_complete_events, f"postgres_{context['ds']}"
                )
                print(
                    f"Marked {len(gold_complete_events)} events as Postgres migration complete"
                )
        finally:
            spark.stop()

    # Task 1: dbt Transformation (Silver → Gold)
    dbt_transformation = DockerOperator(
        task_id="dbt_transformation",
        image=os.getenv("DBT_IMAGE", "juxpkr/geoevent-dbt:0.3"),
        on_success_callback=mark_processing_complete,  # 태스크 성공 직후 호출
        command=[
            "/bin/sh",
            "-c",
            # 1. Seed 초기화 로직
            "if [ ! -f /app/.seed_initialized ]; then dbt seed --full-refresh && touch /app/.seed_initialized; else dbt seed; fi && "
            # 2. dbt 모델 초기화 로직 추가
            "if [ ! -f /app/.dbt_initialized ]; then "
            "    dbt build --target prod --full-refresh && touch /app/.dbt_initialized; "
            "else "
            "    dbt build --target prod; "
            "fi",
        ],
        network_mode="geoevent_data-network",  # docker-compose 네트워크
        mounts=[
            Mount(source=dbt_project_host_path, target="/app", type="bind"),
        ],
        # dbt가 /app 폴더에서 프로젝트를 찾도록 작업 디렉토리 설정
        working_dir="/app",
        environment={
            "DBT_PROFILES_DIR": "/app",
            "DBT_TARGET": "prod",
        },
        cpus=6,  # CPU 코어 6개 사용
        auto_remove="success",  # 실행 후 컨테이너 자동 삭제
        doc_md="""
        dbt Gold Layer Transformation (DockerOperator)
        - Silver Layer 데이터를 분석용 Gold Layer로 변환
        - Actor 코드 매핑 및 설명 추가
        - 비즈니스 로직 적용된 최종 분석 테이블 생성
        - 독립적인 dbt 컨테이너에서 실행
        """,
    )

    # Task 2: Gold -> Postgres 마이그레이션
    # SparkSubmitOperator로 리소스 설정과 통일성 확보
    migrate_to_postgres_task = SparkSubmitOperator(
        task_id="migrate_gold_to_postgres",
        conn_id="spark_conn",
        application="/opt/airflow/src/processing/migration/gdelt_gold_to_postgres.py",
        packages="org.postgresql:postgresql:42.5.0,io.delta:delta-core_2.12:2.4.0",
        on_success_callback=mark_postgres_complete,  # Postgres 마이그레이션 완료 직후 호출
        conf={
            "spark.executor.instances": "5",  # 5개의 작업팀을 투입
            "spark.executor.memory": "8g",  # 각 팀은 8GB 메모리 사용
            "spark.executor.cores": "2",  # 각 팀은 2인 1조로 구성 (총 5*2=10코어)
            "spark.driver.memory": "4g",
        },
        doc_md="""
        Gold Layer to PostgreSQL Migration
        - dbt Gold 테이블들을 PostgreSQL 데이터 마트로 이전
        - 테이블: gdelt_seed_mapping, gdelt_actors_parsed, gdelt_actors_description
        - 마이그레이션 검증 및 상태 리포팅 포함
        """,
    )

    # Task 3: Lifecycle Audit 트리거 (마이그레이션 완료 직후)
    trigger_lifecycle_audit = TriggerDagRunOperator(
        task_id="trigger_lifecycle_audit",
        trigger_dag_id="gdelt_lifecycle_audit",
        wait_for_completion=False,
        doc_md="""
        Lifecycle Audit DAG 트리거
        - Gold-Postgres 마이그레이션 완료 직후 감사 실행
        - 시간차 문제 해결로 동기화 100% 보장
        - Collection Rate, Join Yield, Sync Accuracy 검증
        """,
    )

    # 작업 순서 정의
    (dbt_transformation >> migrate_to_postgres_task >> trigger_lifecycle_audit)
