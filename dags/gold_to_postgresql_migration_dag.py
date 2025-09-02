from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import os
import pendulum

with DAG(
    dag_id="gold_to_postgresql_migration",
    start_date=pendulum.datetime(2025, 8, 28, tz="Asia/Seoul"),
    schedule=None,  # 수동 트리거만
    catchup=False,
    max_active_runs=1,  # 동시 실행 방지
    doc_md="""
    ### Gold to PostgreSQL Migration DAG
    - 목적: dbt Gold Layer → PostgreSQL Data Mart Migration
    - 실행: 수동 트리거 (dbt run 후 실행)
    - 순서: dbt Gold 테이블 → PostgreSQL
    """,
) as dag:
    # 공통 상수 정의
    PROJECT_ROOT = "/opt/airflow"
    SPARK_MASTER = "spark://spark-master:7077"

    # Gold to PostgreSQL Migration Task
    task_migration = BashOperator(
        task_id="migrate_gold_to_postgresql",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"{PROJECT_ROOT}/src/processing/migration/gold-to-postgresql-migration.py"
        ),
        env=dict(os.environ),
    )
