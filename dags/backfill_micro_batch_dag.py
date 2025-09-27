"""
GDELT 마이크로 배치 백필 DAG
1개월을 하루 단위로 쪼개서 처리 - 모니터링과 에러 추적이 쉬워짐
"""

from datetime import datetime, timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# 백필 설정
BACKFILL_START_DATE = datetime(2023, 8, 1)
BACKFILL_END_DATE = datetime(2023, 9, 1)
SEQUENTIAL_PROCESSING = True  # 순차 처리 설정

def generate_hour_list():
    """백필할 시간 리스트 생성 (시간 단위)"""
    hours = []
    current = BACKFILL_START_DATE
    while current < BACKFILL_END_DATE:
        hours.append(current.strftime("%Y-%m-%d-%H"))
        current += timedelta(hours=1)
    return hours

with DAG(
    dag_id='gdelt_micro_batch_backfill',
    start_date=pendulum.now(tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # 동시 실행 Task 제한
    tags=['backfill', 'gdelt', 'micro-batch'],
    doc_md=f"""
    # GDELT 마이크로 배치 백필

    **처리 기간**: {BACKFILL_START_DATE.strftime("%Y-%m-%d")} ~ {BACKFILL_END_DATE.strftime("%Y-%m-%d")}

    ## 전략
    - 1개월을 **하루 단위**로 쪼개서 처리
    - 각 날짜별로 독립적인 Task Group
    - 실패 시 해당 날짜만 재실행
    - 최대 {MAX_PARALLEL_TASKS}개 날짜 동시 처리

    ## 구조
    ```
    daily_2023_08_01 [TaskGroup]
    ├── collect_2023_08_01    # GDELT 서버에서 직접 다운로드
    └── process_2023_08_01    # Bronze → Silver 처리

    daily_2023_08_02 [TaskGroup]
    ├── collect_2023_08_02
    └── process_2023_08_02
    ...
    ```
    """,
) as dag:

    # 시간 리스트 생성
    hours_to_process = generate_hour_list()

    previous_task_group = None

    for i, hour_str in enumerate(hours_to_process):

        with TaskGroup(
            group_id=f'hourly_{hour_str.replace("-", "_")}',
            tooltip=f'Process GDELT data for {hour_str}'
        ) as hourly_group:

            # 1. 데이터 수집 (GDELT → 직접 Bronze)
            collect_task = SparkSubmitOperator(
                task_id=f'collect_{hour_str.replace("-", "_")}',
                conn_id='spark_conn',
                packages="io.delta:delta-core_2.12:2.4.0",
                application="/opt/airflow/src/processing/hourly_gdelt_collector.py",
                application_args=[hour_str],
                env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
                conf={
                    "spark.executor.instances": "1",
                    "spark.executor.memory": "2g",
                    "spark.executor.cores": "1",
                    "spark.driver.memory": "1g",
                },
                doc_md=f"""
                ### {hour_str} 데이터 수집
                - GDELT 서버에서 {hour_str} 시간치 데이터 다운로드 (4개 15분 배치)
                - 직접 Bronze Layer에 저장 (Kafka 우회)
                - Events, Mentions, GKG 모든 타입 처리
                """
            )

            # 2. 데이터 처리 (Bronze → Silver)
            process_task = SparkSubmitOperator(
                task_id=f'process_{hour_str.replace("-", "_")}',
                conn_id='spark_conn',
                packages="io.delta:delta-core_2.12:2.4.0",
                application="/opt/airflow/src/processing/hourly_gdelt_processor.py",
                application_args=[hour_str],
                env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
                conf={
                    "spark.executor.instances": "1",
                    "spark.executor.memory": "3g",
                    "spark.executor.cores": "1",
                    "spark.driver.memory": "2g",
                },
                doc_md=f"""
                ### {hour_str} 데이터 처리
                - Bronze Layer에서 {hour_str} 데이터 읽기
                - 3-Way 조인 (Events + Mentions + GKG)
                - Silver Layer에 저장
                """
            )

            # Task 순서: 수집 → 처리
            collect_task >> process_task

        # 순차 처리 - 이전 시간이 완료된 후 다음 시간 시작
        if SEQUENTIAL_PROCESSING and previous_task_group:
            previous_task_group >> hourly_group

        previous_task_group = hourly_group