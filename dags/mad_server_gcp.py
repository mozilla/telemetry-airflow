"""
Malicious Addons Detection

This runs once a week to emit a trained model to GCS.

Source code is in the private [mad-server repository](https://github.com/mozilla/mad-server/).
"""

from airflow import DAG
from datetime import datetime, timedelta

from utils.gcp import gke_command


default_args = {
    "owner": "gleonard@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 15),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

# TODO set to weekly once ready for production schedule_interval="@weekly",
with DAG("mad_server_gcp",
         default_args=default_args,
         schedule_interval="@hourly",
         # schedule_interval=timedelta(minutes=5),
         doc_md=__doc__,
         tags=['mad'],
         start_date=datetime(2021, 1, 1),
) as dag:
    mad_train_model = gke_command(
        task_id="train_model",
        # TODO GLE this can be removed once branch is merged and 'latest' is used.
        #  Needed since tag != latest.  If it was latest is would always pull by default.
        image_pull_policy='Always',
        cmds=[
            "/bin/bash",
        ],
        command=[
            "bin/train_model",
            "--publish",
            "--publish-as-latest",
            "./working",
        ],
        # TODO GLE need to specify latest version once branch is merged.
        docker_image="gcr.io/srg-team-sandbox/mad-server:latest-gcs",
        startup_timeout_seconds=500,
        # TODO GLE need to update to use prod cluster gcp_conn_id="google_cloud_airflow_gke",
        gcp_conn_id='google_cloud_gke_sandbox',
        # TODO GLE need to update to use prod project gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_project_id='moz-fx-data-gke-sandbox',
        # TODO GLE need to update to use prod cluster gke_cluster_name="workloads-prod-v1",
        gke_cluster_name='gleonard-gke-sandbox',
        gke_location='us-west1',
        env_vars=dict(
            GCS_BUCKET="mad-resources-training",
            GCS_ROOT_TRAINING="datasets",
            CUSTOMS_TRAINING_ALLOW_OVERWRITE="True",
            CLOUD_SERVICE="GCS",
            GCLOUD_PROJECT='mad-model-training'
        ),
        email=[
            "jklukas@mozilla.com",
            "dzeber@mozilla.com",
            "gleonard@mozilla.com",
        ],
    )
