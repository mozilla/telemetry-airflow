from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

from operators.gcp_container_operator import GKEPodOperator
from utils.gcp import bigquery_etl_query, gke_command

DOCS = """\

This DAG is related to data monitoring project and is still under development. Please ignore any alerts related to this DAG 
"""

default_args = {
    "owner": "akommasani@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 1),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = ["repo/telemetry-airflow"]
image = "gcr.io/data-monitoring-dev/dim:latest-app"

with DAG("data_monitoring", default_args=default_args, schedule_interval="@daily", doc_md=DOCS, tags=tags,) as dag:
    run_data_monitoring = GKEPodOperator(
        task_id="run_data_monitoring",
        name="run_data_monitoring",
        image=image,
        arguments=[
            "run",
            "--project=data-monitoring-dev",
            "--dataset=dummy",
            "--table=active_users_aggregates_v1"
            "--date_partition_parameter=2022-01-13"
        ],
        env_vars=dict(
            SLACK_BOT_TOKEN="{{ var.value.dim_slack_secret_token }}"),
        email=["akommasani@mozilla.com"],
        gcp_conn_id='google_cloud_gke_sandbox',
        project_id='moz-fx-data-gke-sandbox',
        cluster_name='akommasani-gke-sandbox',
        location='us-west1',
    )
