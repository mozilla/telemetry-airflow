"""
Runs a Docker image that backfills data from the Google Play store to BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/play-store-export)
"""

from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

project_id = "moz-fx-data-marketing-prod"

tags = [Tag.ImpactTier.tier_3]

with DAG(
    "play_store_export",
    default_args=default_args,
    doc_md=__doc__,
    schedule_interval="@daily",
    tags=tags,
) as dag:

    play_store_export = GKEPodOperator(
        task_id="play_store_export",
        arguments=[
            "python",
            "play_store_export/export.py",
            "--date={{ yesterday_ds }}",
            "--backfill-day-count=60",
            "--project",
            project_id,
            "--transfer-config={{ var.value.play_store_transfer_config_id }}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/play-store-export:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        dag=dag,
        email=[
            "akomar@mozilla.com",
        ],
    )
