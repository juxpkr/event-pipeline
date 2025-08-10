from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

with DAG(
    dag_id="spark_pipeline_dag",
    start_date=pendulum.datetime(2025, 8, 7, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["spark", "pipeline"],
) as dag:
    # 1번 임무: Producer 실행
    run_producer_task = BashOperator(
        task_id="run_kafka_producer",
        bash_command="python /app/producer/producer.py",
        # Airflow 컨테이너 내부에서 실행될 때 사용할 환경 변수 설정
        env={"KAFKA_HOST": "kafka:29092"},
    )

    # 2번 임무: Spark 실행
    run_spark_task = DockerOperator(
        task_id="run_spark_job",
        image="apache/spark:3.5.0",
        command="""
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 2g \
            --conf spark.driver.host=127.0.0.1 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.602,org.postgresql:postgresql:42.7.3 \
            /app/build_travel_datamart.py
        """,
        network_mode="de-pipeline-template_default",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        # source 경로는 캡틴의 PC 실제 경로에 맞게 확인/수정!
        mounts=[
            Mount(
                source="c/Dev/projects/de-pipeline-template", target="/app", type="bind"
            )
        ],
        user="root",
    )

    # 임무 순서 정의: Producer가 성공하면 Spark 실행!
    run_producer_task >> run_spark_task
