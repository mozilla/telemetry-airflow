from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 11),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("mozfun", default_args=default_args, schedule_interval="0 1 * * *") as dag:
    docker_image = "mozilla/bigquery-etl:latest"

    publish_public_udfs = gke_command(
        task_id="publish_public_udfs",
        command=["script/publish_public_udfs"],
        docker_image=docker_image,
        dag=dag
    )
