from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 날짜 및 시간 관련 라이브러리
import pendulum

# TODO: 나중에 SparkSubmitOperator나 DockerOperator로 변경할 수 있도록 구조 잡아두기

with DAG(
    dag_id="gdelt_daily_batch_etl",
    start_date=pendulum.datetime(2025, 8, 17, tz="Asia/Seoul"),
    schedule="@daily",  # 매일 자정에 실행
    catchup=False,
    tags=["gdelt", "batch"],
    doc_md="""
    ### GDELT 일일 배치 ETL DAG
    - 목적: 매일 GDELT 데이터를 처리하여 Silver Layer를 업데이트 한다.
    - 순서: Producer -> Spark -> dbt
    """,
) as dag:
    # --- Task 정의 ---

    # Airflow 컨테이너 내부의 프로젝트 루트 경로
    project_root = "/opt/airflow"

    # 1. GDELT 데이터 추출
    task_run_producer = BashOperator(
        task_id="run_gdelt_producer",
        bash_command=f"python {project_root}/src/ingestion/gdelt/producer_gdelt_microbatch.py",
    )

    # 2. Spark로 데이터 처리
    task_run_spark_processor = BashOperator(
        task_id="run_spark_processor",
        bash_command=f"""
            spark-submit \
            --master spark://spark-master:7077 \
            --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
            {project_root}/src/processing/batch/process_gdelt_data.py
        """,
    )

    # 3. dbt로 데이터 변환 (뼈대만)
    """
    task_run_dbt = BashOperator(
        task_id="run_dbt_transform",
        bash_command=f"cd {project_root}/transforms && dbt run",
    )
    """

    # Task 순서 정의 (의존설 설정)
    # task_run_producer >> task_run_spark_processor >> task_run_dbt
