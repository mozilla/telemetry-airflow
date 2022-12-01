from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    'circular_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    t1 = EmptyOperator(task_id="task1")
    t2 = EmptyOperator(task_id="task2")
    t3 = EmptyOperator(task_id="task3")

    t1 >> [t2, t3] >> t1