"""
Runs a Docker image that produces search alert data.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/search-alert)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 20),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akomar@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "search_alert",
    default_args=default_args,
    doc_md=__doc__,
    schedule_interval="0 4 * * *",
    # We don't want to run more than a single instance of this DAG
    # since underlying tables are not partitioned
    max_active_runs=1,
    tags=tags,
) as dag:
    wait_for_search_aggregates = ExternalTaskSensor(
        task_id="wait_for_search_aggregates",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_aggregates__v8",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    search_alert = GKEPodOperator(
        task_id="search_alert",
        arguments=[
            "python",
            "search_alert/main.py",
            "--submission_date={{ ds }}",
            "--project_id=mozdata",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/search-alert_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )

    wait_for_search_aggregates >> search_alert
