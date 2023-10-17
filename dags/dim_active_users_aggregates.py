"""
DAG for running simple cross-check for product usage KPI metrics.

Part of DENG-750 - first draft at how we could be executing a simple cross-check
for tables used for product usage KPI metrics.

This is WIP, and it meant for testing only. However, the failures recorded by
this DAG are important and the standatd triage process should be followed
as long as this DAG is active.
"""

from collections import namedtuple
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime(2023, 3, 20),
    "email": [
        "telemetry-alerts@mozilla.com",
        "kik@mozilla.com",
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

Config = namedtuple("Config", "exec_date apps")
CONFIGS = {
    "desktop": Config("{{ ds }}", ("firefox_desktop",)),
    "mobile": Config(
        "{{ macros.ds_add(ds, -1) }}",
        (
            "fenix",
            "focus_android",
            "firefox_ios",
            "focus_ios",
            "klar_ios",
        ),
    ),
}

PROJECT_ID = "mozdata"
APP_NAME_POSTFIX = "active_users_aggregates"

for platform, config in CONFIGS.items():
    dag_id = f"dim_active_users_aggregates_{platform}"
    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval="30 4 * * *",
        tags=TAGS,
        start_date=datetime(2023, 3, 20),
        doc_md=__doc__,
    ) as dag:
        run_all = EmptyOperator(
            task_id="run_all",
        )

    for app_name in config.apps:
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
                f"--date={config.exec_date}",
            ],
            env_vars={"SLACK_BOT_TOKEN": "{{ var.value.dim_slack_secret_token }}"},
            gcp_conn_id="google_cloud_airflow_gke",
            project_id="moz-fx-data-airflow-gke-prod",
            cluster_name="workloads-prod-v1",
            location="us-west1",
        )

        run_all >> wait_for_aggregates >> dim_check
    globals()[dag_id] = dag
