"""
DAG for running simple cross-check for product usage KPI metrics.

Part of DENG-750 - first draft at how we could be executing a simple cross-check
for tables used for product usage KPI metrics.

This is WIP, and it meant for testing only. However, the failures recorded by
this DAG are important and the standatd triage process should be followed
as long as this DAG is active.
"""


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "kignasiak@mozilla.com",
    "start_date": datetime(2023, 3, 20),
    "email": [
        "telemetry-alerts@mozilla.com",
        "kignasiak@mozilla.com",
        "lvargas@mozilla.com",
    ],
    "email_on_failure": True,
    "retries": 0,
}

TAGS = [
    "repo/telemetry-airflow",
    Tag.ImpactTier.tier_1,
]
IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/dim:latest"

APP_NAME_POSTFIX = "active_users_aggregates"
APPS = ("firefox_desktop",)

PROJECT_ID = "mozdata"
EXEC_DATE = "{{ macros.ds_add(ds, -1) }}"

with DAG(
    "dim_active_users_aggregates",
    default_args=default_args,
    schedule_interval="30 4 * * *",
    tags=TAGS,
    doc_md=__doc__,
) as dag:

    run_all = DummyOperator(
        task_id="run_all",
    )

    for app_name in APPS:
        wait_for_aggregates = ExternalTaskSensor(
            task_id=f"wait_for_{app_name}",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id=f"{app_name}_active_users_aggregates",
            execution_delta=timedelta(seconds=3600),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )

        dim_check = GKEPodOperator(
            task_id=f"{app_name}_{APP_NAME_POSTFIX}_dim_check",
            name=f"{app_name}_{APP_NAME_POSTFIX}_dim_check",
            image=IMAGE,
            arguments=[
                "dim",
                "run",
                "--fail_process_on_failure",
                f"--project_id={PROJECT_ID}",
                f"--dataset={app_name}",
                "--table=active_users_aggregates",
                f"--date={EXEC_DATE}",
            ],
            env_vars={"SLACK_BOT_TOKEN": "{{ var.value.dim_slack_secret_token }}"},
            gcp_conn_id="google_cloud_airflow_gke",
            project_id="moz-fx-data-airflow-gke-prod",
            cluster_name="workloads-prod-v1",
            location="us-west1",
        )

        run_all >> wait_for_aggregates >> dim_check
