from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command

from operators.task_sensor import ExternalTaskCompletedSensor


docs = """
### operational_monitoring

This DAG is currently under development. Failures can be ignored during Airflow triage.

#### Description

This DAG schedules queries for populating datasets used for operational monitoring.
The queries are generated via [`operational_monitoring` in bigquery-etl](https://github.com/mozilla/bigquery-etl/tree/main/bigquery_etl/operational_monitoring). 

#### Owner

msamuel@mozilla.com
"""

default_args = {
    "owner": "msamuel@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 3),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}
DAG_NAME = "operational_monitoring"

with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
) as dag:
    wait_for_main_nightly = ExternalTaskCompletedSensor(
        task_id="wait_for_main_nightly",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_nightly__v1",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    operational_monitoring = gke_command(
        task_id="run_operational_monitoring",
        command=[
            "./script/bqetl",
            "opmon",
            "run",
            "--submission-date={{ ds }}",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        dag=dag,
    )

    wait_for_main_nightly >> operational_monitoring
