from datetime import datetime

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
This DAG is related to data monitoring project it is still under development.
All alerts related to this DAG can be ignored.

(for more info on dim see: https://github.com/mozilla/dim)

*Triage notes*

This process is WIP and the DAG is currently set up on purpose to fail unless
all data checks pass. Please *ignore all failures* for this DAG and do not change
their status. Once we have a proper dashboard build to track and observe when
things fail we will update the DAG.
"""

# telemetry_derived for the same tables as in telemetry are needed as different tests are executed
# for example, the views replace country values at runtime hence we can only verify its value
# using the view, the tables have associated metadata around partition sizes which we use for
# one of the checks.

TARGET_DATASETS = (
    "moz-fx-data-shared-prod.telemetry.unified_metrics",
    "moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1",
    "moz-fx-data-shared-prod.telemetry.cohort_daily_statistics",
    "moz-fx-data-shared-prod.telemetry_derived.cohort_daily_statistics_v1",
    # "moz-fx-data-shared-prod.telemetry.rolling_cohorts",  # removed as no longer needed: DENG-2064
    # "moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v1",  # removed as no longer needed: DENG-2064
    "moz-fx-data-shared-prod.telemetry.active_users_aggregates",
    "moz-fx-data-shared-prod.telemetry_derived.active_users_aggregates_v1",
    # "moz-fx-data-shared-prod.internet_outages.global_outages_v1", # Migrated over to ETL checks: DENG-2045
    "moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_last_seen",
)

default_args = {
    "owner": "akommasani@mozilla.com",
    "start_date": datetime(2023, 5, 26),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

TAGS = ["repo/telemetry-airflow", Tag.ImpactTier.tier_1, Tag.Triage.no_triage]
IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/dim:latest"

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
                "dim",
                "run",
                "--fail_process_on_failure",
                f"--project_id={project_id}",
                f"--dataset={dataset}",
                f"--table={table}",
                "--date={{ macros.ds_add(ds, -1) }}",
            ],
            env_vars={"SLACK_BOT_TOKEN": "{{ var.value.dim_slack_secret_token }}"},
            gcp_conn_id="google_cloud_airflow_gke",
            project_id="moz-fx-data-airflow-gke-prod",
            cluster_name="workloads-prod-v1",
            location="us-west1",
        )
