from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow_dag",
    start_date=pendulum.datetime(2025, 8, 6, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    hello_task = BashOperator(
        task_id="hello_task",
        bash_command='echo "Hello from Airflow! We did it, Captain!"',
    )
