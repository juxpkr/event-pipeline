from __future__ import annotations
import os
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
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

    # Task 1: dbt Transformation (Silver → Gold)
    dbt_transformation = DockerOperator(
        task_id="dbt_transformation",
        image=os.getenv("DBT_IMAGE", "juxpkr/geoevent-dbt:0.3"),
        command=[
            "/bin/sh",
            "-c",
            "dbt build --target prod --select +gold_dashboard_master",
        ],
        network_mode="geoevent_data-network",  # docker-compose 네트워크
        mounts=[
            Mount(source=dbt_project_host_path, target="/app", type="bind"),
        ],
        # dbt가 /app 폴더에서 프로젝트를 찾도록 작업 디렉토리 설정
        working_dir="/app",
        environment={
            "DBT_PROFILES_DIR": "/app",
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

    # 2단계: Gold -> Postgres 마이그레이션
    # SparkSubmitOperator로 리소스 설정과 통일성 확보
    migrate_to_postgres_task = SparkSubmitOperator(
        task_id="migrate_gold_to_postgres",
        conn_id="spark_conn",
        application="/opt/airflow/src/processing/migration/gdelt_gold_to_postgres.py",
        packages="org.postgresql:postgresql:42.5.0",
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

    # Note: 데이터 감사는 gdelt_lifecycle_audit_dag에서 처리됨 (마이그레이션 완료 1분 후)

    # 작업 순서 정의
    dbt_transformation >> migrate_to_postgres_task
