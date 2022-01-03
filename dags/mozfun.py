"""
Nightly deploy of `mozfun` UDFs.

This job has elevated permissions to be able to create new datasets in the mozfun
project when new logical namespaces are created in the mozfun directory of bigquery-etl.
This is the reason that individual data engineers cannot deploy UDFs to the mozfun project
on their own, hence the need for this nightly task.
"""

from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 11),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG("mozfun", default_args=default_args, schedule_interval="@daily", doc_md=__doc__, tags=tags,) as dag:
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

    publish_public_udfs = gke_command(
        task_id="publish_public_udfs",
        command=["script/publish_public_udfs"],
        docker_image=docker_image
    )
