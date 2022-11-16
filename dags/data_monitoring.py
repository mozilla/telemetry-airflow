from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

from operators.gcp_container_operator import GKEPodOperator
from utils.gcp import bigquery_etl_query, gke_command

DOCS = """\
This DAG is related to data monitoring project it is still under development.
All alerts related to this DAG can be ignored.

(for more info on dim see: https://github.com/mozilla/dim)
"""

TARGET_DATASETS = (
    "moz-fx-data-shared-prod.telemetry.unified_metrics",
    "moz-fx-data-shared-prod.telemetry.cohort_daily_statistics",
    "moz-fx-data-shared-prod.telemetry.rolling_cohorts",
    "moz-fx-data-shared-prod.telemetry.active_users_aggregates",
    "moz-fx-data-shared-prod.internet_outages.global_outages_v1",
    "moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_last_seen",
)

default_args = {
    "owner": "akommasani@mozilla.com",
    "start_date": datetime(2022, 11, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

TAGS = ["repo/telemetry-airflow", "impact/tier_3",]
IMAGE = "gcr.io/data-monitoring-dev/dim:latest-app"

with DAG(
    "data_monitoring",
    default_args=default_args,
    schedule_interval="0 7 * * *",  # all bqetl jobs should have finished by 7am UTC
    doc_md=DOCS,
    tags=TAGS,
    catchup=True,
) as dag:

    for target_dataset in TARGET_DATASETS:
        project_id, dataset, table = target_dataset.split(".")

        task_name = f"dim_run_check_{target_dataset}"

        run_data_monitoring = GKEPodOperator(
            task_id=task_name,
            name=task_name,
            image=IMAGE,
            arguments=[
                "run",
                "--fail_process_on_failure",
                f"--project_id={project_id}",
                f"--dataset={dataset}",
                f"--table={table}"
                "--date={{ macros.ds_add(ds, -1) }}"
            ],
            env_vars=dict(
                SLACK_BOT_TOKEN="{{ var.value.dim_slack_secret_token }}"),
            gcp_conn_id='google_cloud_airflow_gke',
            project_id='moz-fx-data-airflow-gke-prod',
            cluster_name='workloads-prod-v1',
            location='us-west1',
        )
