from __future__ import annotations
import os
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="gdelt_silver_to_gold",
    start_date=pendulum.now(tz="Asia/Seoul"),
    schedule=None,  # 직접 실행하거나, 다른 DAG이 호출하도록 설정
    catchup=False,
    doc_md="""
    Silver Layer 데이터를 dbt로 변환하여 Gold Layer를 만들고,
    최종 결과를 PostgreSQL 데이터 마트로 이전합니다.
    """,
) as dag:
    # Airflow 워커 컨테이너 내부에 있는 dbt 프로젝트 경로를 변수로 지정
    dbt_project_host_path = f"{os.getenv('PROJECT_ROOT', '/opt/airflow')}/transforms"

    # Task 1: dbt Transformation (Silver → Gold)
    dbt_transformation = DockerOperator(
        task_id="dbt_transformation",
        # image="juxpkr/geoevent-dbt:0.1",
        image=os.getenv("DBT_IMAGE", "juxpkr/geoevent-dbt:0.1"),
        command="dbt build --select +stg_seed_mapping +stg_seed_actors_parsed +stg_actors_description",
        network_mode="geoevent_data-network",  # docker-compose 네트워크
        mounts=[
            # PWD 대신, 환경변수를 사용
            f"{os.getenv('PROJECT_ROOT')}/transforms:/app",  # dbt 프로젝트 마운트 (Swarm 호환)
        ],
        # dbt가 파일을 찾을 수 있도록 두 개의 통로를 열어준다
        volumes=[
            # 1: dbt 프로젝트 전체를 /app 폴더에 연결
            f"{dbt_project_host_path}:/app",
            # 2: profiles.yml를 dbt가 원하는 경로에 직접 연결
            f"{dbt_project_host_path}/profiles.yml:/home/dbt_user/.dbt/profiles.yml",
        ],
        # dbt가 /app 폴더에서 프로젝트를 찾도록 작업 디렉토리 설정
        working_dir="/app",
        environment={
            "DBT_PROFILES_DIR": "/app",
        },
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
    # 이것도 BashOperator나 DockerOperator로 실행
    migrate_to_postgres_task = BashOperator(
        task_id="migrate_gold_to_postgres",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --packages org.postgresql:postgresql:42.5.0 \
        /opt/airflow/src/processing/migration/gdelt_gold_to_postgres.py
        """,
        doc_md="""
        Gold Layer to PostgreSQL Migration
        - dbt Gold 테이블들을 PostgreSQL 데이터 마트로 이전
        - 테이블: gdelt_seed_mapping, gdelt_actors_parsed, gdelt_actors_description
        - 마이그레이션 검증 및 상태 리포팅 포함
        """,
    )

    # 작업 순서 정의
    dbt_transformation >> migrate_to_postgres_task
