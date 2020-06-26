from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command


default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

project_id = "moz-fx-data-marketing-prod"

with DAG("play_store_export",
         default_args=default_args,
         schedule_interval="@daily") as dag:

    play_store_export = gke_command(
        task_id="play_store_export",
        command=[
            "python", "play_store_export/export.py",
            "--date={{ ds }}",
            "--backfill-day-count=35",
            "--project", project_id,
            "--transfer-config={{ var.value.play_store_transfer_config_id }}",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/play-store-export:latest",
        gcp_conn_id="google_cloud_derived_datasets",
        dag=dag,
        email=[
            "bewu@mozilla.com",
        ],
    )
