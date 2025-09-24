"""
GDELT Lifecycle Audit DAG
Event lifecycle 기반 데이터 감사 시스템
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
import docker

# DAG 기본 설정
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gdelt_lifecycle_audit',
    default_args=default_args,
    description='GDELT Event Lifecycle Audit System',
    schedule_interval='18,33,48,3 * * * *',  # 15분마다 실행 (18분, 33분, 48분, 03분)
    catchup=False,
    max_active_runs=1,
    tags=['gdelt', 'audit', 'monitoring']
)

# Lifecycle Audit 실행
lifecycle_audit_task = DockerOperator(
    task_id='run_lifecycle_audit',
    image='event-pipeline-spark-base:latest',
    command=[
        'python', '/opt/airflow/src/validation/lifecycle_auditor.py',
        '--hours-back', '24'
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='event-pipeline_data-network',
    mounts=[
        docker.types.Mount(
            target='/var/run/docker.sock',
            source='/var/run/docker.sock',
            type='bind'
        ),
        docker.types.Mount(
            target='/opt/airflow',
            source='event-pipeline_shared-data',
            type='volume'
        )
    ],
    environment={
        'SPARK_MASTER_URL': 'spark://spark-master:7077',
        'PROMETHEUS_PUSHGATEWAY_URL': 'http://pushgateway:9091'
    },
    dag=dag
)

# 올드 이벤트 자동 만료
expire_old_events_task = DockerOperator(
    task_id='expire_old_events',
    image='event-pipeline-spark-base:latest',
    command=[
        'python', '-c',
        '''
from src.audit.lifecycle_tracker import EventLifecycleTracker
from src.utils.spark_builder import get_spark_session
spark = get_spark_session("Expire_Old_Events", "spark://spark-master:7077")
tracker = EventLifecycleTracker(spark)
expired = tracker.expire_old_waiting_events(48)  # 48시간 이상
print(f"Expired {expired} old events")
spark.stop()
        '''
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='event-pipeline_data-network',
    mounts=[
        docker.types.Mount(
            target='/var/run/docker.sock',
            source='/var/run/docker.sock',
            type='bind'
        ),
        docker.types.Mount(
            target='/opt/airflow',
            source='event-pipeline_shared-data',
            type='volume'
        )
    ],
    environment={
        'SPARK_MASTER_URL': 'spark://spark-master:7077'
    },
    dag=dag
)

# 태스크 의존성
lifecycle_audit_task >> expire_old_events_task