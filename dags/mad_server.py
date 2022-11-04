"""
Malicious Addons Detection

This runs once a week to emit a trained model to GCS.

Source code is in the private [mad-server repository](https://github.com/mozilla/mad-server/).
"""

from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command
from utils.tags import Tag


default_args = {
    "owner": "gleonard@mozilla.com",
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
gcloud_project = 'mad-model-training'
gcs_report_bucket = 'mad-reports'

with DAG("mad_server", default_args=default_args, schedule_interval="@weekly", doc_md=__doc__, tags=tags,) as dag:

    mad_server_pull = gke_command(
        task_id="mad_server_pull",
        # Controls the entrypoint of the container, which for mad-server
        # defaults to bin/run rather than a shell.
        cmds=[
            "/bin/bash",
        ],
        command=[
            "bin/airflow-pull",
        ],
        docker_image="us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/mad-server:latest",
        startup_timeout_seconds=500,
        gcp_conn_id="google_cloud_airflow_gke",
        gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_cluster_name="workloads-prod-v1",
        gke_location="us-west1",
        env_vars=dict(
            GCS_BUCKET=gcs_bucket,
            GCS_ROOT_TRAINING=gcs_root_training,
            CLOUD_SERVICE=cloud_service,
            CUSTOMS_TRAINING_ALLOW_OVERWRITE=customs_training_allow_overwrite,
            AMO_CRED_ISSUER="{{ var.value.AMO_CRED_ISSUER }}",
            AMO_CRED_SECRET="{{ var.value.AMO_CRED_SECRET }}",
        ),
        email=[
            "jklukas@mozilla.com",
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
    )
    mad_train_model = gke_command(
        task_id="train_model",
        cmds=[
            "/bin/bash",
        ],
        command=[
            "bin/train_model",
            "--publish",
            "--publish-as-latest",
            "./working",
        ],
        docker_image="us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/mad-server:latest",
        startup_timeout_seconds=500,
        gcp_conn_id="google_cloud_airflow_gke",
        gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_cluster_name="workloads-prod-v1",
        gke_location='us-west1',
        env_vars=dict(
            GCS_BUCKET=gcs_bucket,
            GCS_ROOT_TRAINING=gcs_root_training,
            CLOUD_SERVICE=cloud_service,
            CUSTOMS_TRAINING_ALLOW_OVERWRITE=customs_training_allow_overwrite,
            GCLOUD_PROJECT=gcloud_project,
            GCS_REPORT_BUCKET=gcs_report_bucket,
        ),
        email=[
            "jklukas@mozilla.com",
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
    )
    new_data_eval = gke_command(
        task_id="evaluate_new_data",
        cmds=[
            "/bin/bash",
        ],
        command=[
            "bin/evaluate_new_data",
            "--publish",
            "--publish-as-latest",
            "./working",
        ],
        docker_image="us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/mad-server:latest",
        startup_timeout_seconds=500,
        gcp_conn_id="google_cloud_airflow_gke",
        gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_cluster_name="workloads-prod-v1",
        gke_location='us-west1',
        env_vars=dict(
            GCS_BUCKET=gcs_bucket,
            GCS_ROOT_TRAINING=gcs_root_training,
            CLOUD_SERVICE=cloud_service,
            CUSTOMS_TRAINING_ALLOW_OVERWRITE=customs_training_allow_overwrite,
            GCLOUD_PROJECT=gcloud_project,
            GCS_REPORT_BUCKET=gcs_report_bucket,
        ),
        email=[
            "jklukas@mozilla.com",
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
    )

    mad_server_pull >> mad_train_model >> new_data_eval
