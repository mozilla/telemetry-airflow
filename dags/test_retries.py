from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    "test_retries",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 11, 7, 22, 20, tzinfo=timezone.utc),
    catchup=False,
) as dag:
    BashOperator(
        task_id='failing_task',
        bash_command='sleep 30 && exit 1',
    )
