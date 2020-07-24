from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.gcp import gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 30),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("monitoring", default_args=default_args, schedule_interval="0 2 * * *") as dag:
    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    stable_table_sizes = gke_command(
        task_id="stable_table_sizes",
        command=[
            "python",
            "sql/monitoring/stable_table_sizes_v1/query.py",
            "--date",
            "{{ ds }}",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"])

    stable_table_sizes.set_upstream(wait_for_copy_deduplicate_main_ping)
    stable_table_sizes.set_upstream(wait_for_copy_deduplicate_all)
