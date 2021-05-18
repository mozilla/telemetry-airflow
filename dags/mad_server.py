from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command


default_args = {
    "owner": "jklukas@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 15),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

with DAG("mad_server", default_args=default_args, schedule_interval="@daily") as dag:

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
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/mad-server:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        gke_cluster_name="workloads-prod-v1",
        gke_location="us-west1",
        aws_conn_id="aws_dev_mad_resources_training",
        env_vars=dict(
            S3_BUCKET="mad-resources-training",
            S3_ROOT_TRAINING="datasets",
            CUSTOMS_TRAINING_ALLOW_OVERWRITE="True",
            AMO_CRED_ISSUER="{{ var.value.AMO_CRED_ISSUER }}",
            AMO_CRED_SECRET="{{ var.value.AMO_CRED_SECRET }}",
        ),
        email=[
            "jklukas@mozilla.com",
            "dzeber@mozilla.com",
            "alissy@mozilla.com",
        ],
    )
