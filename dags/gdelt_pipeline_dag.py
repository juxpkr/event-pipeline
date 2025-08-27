from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 날짜 및 시간 관련 라이브러리
import pendulum

with DAG(
    dag_id="gdelt_pipeline_full",
    start_date=pendulum.datetime(2025, 8, 26, tz="Asia/Seoul"),
    schedule="@hourly",  # 매시간 실행
    catchup=False,
    tags=["gdelt", "pipeline", "kafka", "spark"],
    doc_md="""
    ### GDELT 전체 파이프라인 DAG
    - 목적: Raw Producer → Kafka → Spark Processor → MinIO Silver Table
    - 순서: Raw Producer → Silver Processor
    """,
) as dag:
    # --- Task 정의 ---

    # Airflow 컨테이너 내부의 프로젝트 루트 경로
    project_root = "/opt/airflow"

    # 1. GDELT Raw 데이터 Producer (Kafka로 전송)
    task_raw_producer = BashOperator(
        task_id="gdelt_raw_producer",
        bash_command=f"python {project_root}/src/ingestion/gdelt/producer_gdelt_raw.py",
    )

    # 2. Spark Processor (Kafka → MinIO Silver Table)
    task_silver_processor = BashOperator(
        task_id="gdelt_silver_processor",
        bash_command=f"python {project_root}/src/processing/batch/gdelt_silver_processor.py",
    )

    # Task 순서 정의 (Producer → Processor)
    task_raw_producer >> task_silver_processor
