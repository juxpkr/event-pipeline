from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import pendulum

with DAG(
    dag_id="gold_to_postgresql_migration",
    start_date=pendulum.datetime(2025, 8, 28, tz="Asia/Seoul"),
    schedule=None,  # 수동 트리거만
    catchup=False,
    tags=["gdelt", "migration", "postgresql", "gold"],
    doc_md="""
    ### Gold to PostgreSQL Migration DAG
    - 목적: dbt Gold Layer → PostgreSQL Data Mart Migration
    - 실행: 수동 트리거 (dbt run 후 실행)
    - 순서: dbt Gold 테이블 → PostgreSQL
    """,
) as dag:
    # Airflow 컨테이너 내부의 프로젝트 루트 경로
    project_root = "/opt/airflow"

    # Gold to PostgreSQL Migration Task
    task_migration = BashOperator(
        task_id="migrate_gold_to_postgresql",
        bash_command=f"python {project_root}/src/processing/migration/gold-to-postgresql-migration.py",
    )
