"""
Malicious Addons Detection.

This runs once a week to emit a trained model to GCS.

Source code is in the private [mad-server repository](https://github.com/mozilla/mad-server/).

*Triage notes*

The way the app was designed it is decoupled from Airflow and will pull all data since the last
successful data pull. What this means if we have a failed DAG run followed by
a successful DAG run it will cover the data from the previous run.

So as long as the most recent DAG run is successful the job can be considered healthy
and not action is required for failed DAG runs.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "dzeber@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 15),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

gcs_bucket = "mad-resources-training"
gcs_root_training = "datasets"
cloud_service = "GCS"
customs_training_allow_overwrite = "True"
gcloud_project = "mad-model-training"
gcs_report_bucket = "mad-reports"
amo_cred_issuer_secret = Secret(
    deploy_type="env",
    deploy_target="AMO_CRED_ISSUER",
    secret="airflow-gke-secrets",
    key="mad_server_secret__amo_cred_issuer"
)
amo_cred_secret_secret = Secret(
    deploy_type="env",
    deploy_target="AMO_CRED_SECRET",
    secret="airflow-gke-secrets",
    key="mad_server_secret__amo_cred_secret"
)

with DAG(
    "mad_server",
    default_args=default_args,
    schedule_interval="@weekly",
    doc_md=__doc__,
    tags=tags,
) as dag:
    mad_server_pull = GKEPodOperator(
        task_id="mad_server_pull",
        # Controls the entrypoint of the container, which for mad-server
        # defaults to bin/run rather than a shell.
        cmds=[
            "/bin/bash",
        ],
        arguments=[
            "bin/airflow-pull",
        ],
        image="us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/mad-server:latest",
        startup_timeout_seconds=500,
        gcp_conn_id="google_cloud_airflow_gke",
        env_vars={
            "GCS_BUCKET": gcs_bucket,
            "GCS_ROOT_TRAINING": gcs_root_training,
            "CLOUD_SERVICE": cloud_service,
            "CUSTOMS_TRAINING_ALLOW_OVERWRITE": customs_training_allow_overwrite,
        },
        email=[
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
        secrets=[amo_cred_issuer_secret, amo_cred_secret_secret],
    )
    mad_train_model = GKEPodOperator(
        task_id="train_model",
        cmds=[
            "/bin/bash",
        ],
        arguments=[
            "bin/train_model",
            "--publish",
            "--publish-as-latest",
            "./working",
        ],
        image="us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/mad-server:latest",
        startup_timeout_seconds=500,
        env_vars={
            "GCS_BUCKET": gcs_bucket,
            "GCS_ROOT_TRAINING": gcs_root_training,
            "CLOUD_SERVICE": cloud_service,
            "CUSTOMS_TRAINING_ALLOW_OVERWRITE": customs_training_allow_overwrite,
            "GCLOUD_PROJECT": gcloud_project,
            "GCS_REPORT_BUCKET": gcs_report_bucket,
        },
        email=[
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
    )
    new_data_eval = GKEPodOperator(
        task_id="evaluate_new_data",
        cmds=[
            "/bin/bash",
        ],
        arguments=[
            "bin/evaluate_new_data",
            "--publish",
            "--publish-as-latest",
            "./working",
        ],
        image="us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/mad-server:latest",
        startup_timeout_seconds=500,
        gcp_conn_id="google_cloud_airflow_gke",
        env_vars={
            "GCS_BUCKET": gcs_bucket,
            "GCS_ROOT_TRAINING": gcs_root_training,
            "CLOUD_SERVICE": cloud_service,
            "CUSTOMS_TRAINING_ALLOW_OVERWRITE": customs_training_allow_overwrite,
            "GCLOUD_PROJECT": gcloud_project,
            "GCS_REPORT_BUCKET": gcs_report_bucket,
        },
        email=[
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
    )

    mad_server_pull >> mad_train_model >> new_data_eval
