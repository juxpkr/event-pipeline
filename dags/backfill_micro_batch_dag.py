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
MAX_PARALLEL_TASKS = 3  # 동시 실행할 일자 수

def generate_date_list():
    """백필할 날짜 리스트 생성"""
    dates = []
    current = BACKFILL_START_DATE
    while current < BACKFILL_END_DATE:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates

with DAG(
    dag_id='gdelt_micro_batch_backfill',
    start_date=pendulum.now(tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
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

    # 날짜 리스트 생성
    dates_to_process = generate_date_list()

    previous_task_group = None

    for i, date_str in enumerate(dates_to_process):

        with TaskGroup(
            group_id=f'daily_{date_str.replace("-", "_")}',
            tooltip=f'Process GDELT data for {date_str}'
        ) as daily_group:

            # 1. 데이터 수집 (GDELT → 직접 Bronze)
            collect_task = SparkSubmitOperator(
                task_id=f'collect_{date_str.replace("-", "_")}',
                conn_id='spark_conn',
                packages="io.delta:delta-core_2.12:2.4.0",
                application="/opt/airflow/src/processing/daily_gdelt_collector.py",
                application_args=[date_str],
                env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
                conf={
                    "spark.executor.instances": "2",
                    "spark.executor.memory": "4g",
                    "spark.executor.cores": "1",
                    "spark.driver.memory": "2g",
                },
                doc_md=f"""
                ### {date_str} 데이터 수집
                - GDELT 서버에서 {date_str} 하루치 데이터 다운로드
                - 직접 Bronze Layer에 저장 (Kafka 우회)
                - Events, Mentions, GKG 모든 타입 처리
                """
            )

            # 2. 데이터 처리 (Bronze → Silver)
            process_task = SparkSubmitOperator(
                task_id=f'process_{date_str.replace("-", "_")}',
                conn_id='spark_conn',
                packages="io.delta:delta-core_2.12:2.4.0",
                application="/opt/airflow/src/processing/daily_gdelt_processor.py",
                application_args=[date_str],
                env_vars={"REDIS_HOST": "redis", "REDIS_PORT": "6379"},
                conf={
                    "spark.executor.instances": "3",
                    "spark.executor.memory": "6g",
                    "spark.executor.cores": "2",
                    "spark.driver.memory": "4g",
                },
                doc_md=f"""
                ### {date_str} 데이터 처리
                - Bronze Layer에서 {date_str} 데이터 읽기
                - 3-Way 조인 (Events + Mentions + GKG)
                - Silver Layer에 저장
                """
            )

            # Task 순서: 수집 → 처리
            collect_task >> process_task

        # 병렬 처리 제한 (3개씩 처리)
        if previous_task_group and i % MAX_PARALLEL_TASKS == 0:
            previous_task_group >> daily_group

        previous_task_group = daily_group