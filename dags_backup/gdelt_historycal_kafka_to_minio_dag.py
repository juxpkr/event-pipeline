from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="gdelt_historical_kafka_to_minio_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    doc_md="""
    ### GDELT Kafka to MinIO DAG

    이 DAG는 다음 두 가지 작업을 순차적으로 수행합니다.
    1.  **GDELT Producer**: BigQuery에서 GDELT 데이터를 가져와 Kafka 'gdelt_events' 토픽으로 전송합니다.
    2.  **Kafka to MinIO Consumer**: 'gdelt_events' 토픽의 데이터를 소비하여 MinIO 'raw' 버킷에 JSON 파일로 저장합니다.
    """,
    tags=["ingestion", "gdelt", "kafka", "minio"],
) as dag:
    # 컨테이너 내부의 프로젝트 루트 경로
    project_root_in_container = "/opt/airflow"

    # 1. gdelt_producer.py 실행
    run_producer = BashOperator(
        task_id="run_gdelt_producer",
        bash_command=f"python {project_root_in_container}/src/ingestion/gdelt/gdelt_bigq_to_kafka.py",
    )

    # 2. kafka_to_minio_consumer.py 실행
    run_consumer = BashOperator(
        task_id="run_kafka_to_minio_consumer",
        bash_command=f"python {project_root_in_container}/src/ingestion/gdelt/kafka_bigq_to_minio_consumer.py",
    )

    # 작업 순서 설정: Producer 실행 후 Consumer 실행
    run_producer >> run_consumer
