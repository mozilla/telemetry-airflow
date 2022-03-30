from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command
from utils.tags import Tag
from operators.gcp_container_operator import GKEPodOperator

from operators.task_sensor import ExternalTaskCompletedSensor


docs = """
### operational_monitoring


This DAG schedules queries for populating datasets used for operational monitoring.
The queries are generated via [`opmon`](https://github.com/mozilla/opmon). 

#### Owner

ascholtz@mozilla.com
"""

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 3),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

DAG_NAME = "operational_monitoring"
tags = [Tag.ImpactTier.tier_3]

with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    # Built from repo https://github.com/mozilla/opmon
    opmon_image = "gcr.io/moz-fx-data-experiments/opmon:latest"

    opmon_run = GKEPodOperator(
        task_id="opmon_run",
        name="opmon_run",
        image=opmon_image,
        email=["ascholtz@mozilla.com"],
        arguments=[
            "--log_to_bigquery",
            "run", 
            "--date={{ ds }}",
        ],
        dag=dag,
    )

    wait_for_clients_daily_export = ExternalTaskCompletedSensor(
        task_id="wait_for_clients_daily",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_main_summary_export = ExternalTaskCompletedSensor(
        task_id="wait_for_main_summary",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_summary__v4",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_search_clients_daily = ExternalTaskCompletedSensor(
        task_id="wait_for_search_clients_daily",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    opmon_run.set_upstream(
        [
            wait_for_clients_daily_export,
            wait_for_main_summary_export,
            wait_for_search_clients_daily,
        ]
    )
