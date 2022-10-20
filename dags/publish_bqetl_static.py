"""
Daily deployment of static bigquery-etl data to various projects.

See the publish command [here](https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/static/__init__.py).
"""

from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command
from utils.tags import Tag

IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

default_args = {
    "owner": "anicholson@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "anicholson@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 4),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "publish_bqetl_static",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=__doc__,
    tags=tags,
) as dag:

    publish_static_mozdata = gke_command(
        task_id="publish_static_mozdata",
        command=[
            "script/bqetl", "static", "publish",
            "--project_id", "mozdata"
        ],
        docker_image=IMAGE,
    )

    publish_static_shared_prod = gke_command(
        task_id="publish_static_shared_prod",
        command=[
            "script/bqetl", "static", "publish",
            "--project_id", "moz-fx-data-shared-prod"
        ],
        docker_image=IMAGE,
    )
