"""
Nightly deploy of bigquery etl views.
"""

from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
        "dthorn@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 6),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG("bqetl_artifact_deployment", default_args=default_args, schedule_interval="@daily", doc_md=__doc__, tags=tags,) as dag:
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

    publish_public_udfs = gke_command(
        task_id="publish_public_udfs",
        command=["script/publish_public_udfs"],
        docker_image=docker_image
    )

    publish_persistent_udfs = gke_command(
        task_id="publish_persistent_udfs",
        cmds=["bash", "-c"],
        command=[
            "script/publish_persistent_udfs --project-id=moz-fx-data-shared-prod && "
            "script/publish_persistent_udfs --project-id=mozdata"
        ],
        docker_image=docker_image,
    )

    publish_new_tables = gke_command(
        task_id="publish_new_tables",
        cmds=["bash", "-c"],
        command=[
            "script/bqetl generate all && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip &&"
            "script/bqetl query schema deploy '*' --skip-existing --use-cloud-function=false --force --ignore-dryrun-skip"
        ],
        docker_image=docker_image,
    )

    publish_views = gke_command(
        task_id="publish_views",
        cmds=["bash", "-c"],
        command=[
            "script/bqetl generate all && "
            "script/bqetl view publish --target-project=moz-fx-data-shared-prod && "
            "script/bqetl view publish --target-project=mozdata --user-facing-only && "
            "script/publish_public_data_views --target-project=moz-fx-data-shared-prod && "
            "script/publish_public_data_views --target-project=mozdata"
        ],
        docker_image=docker_image,
    )

    publish_views.set_upstream(publish_public_udfs)
    publish_views.set_upstream(publish_persistent_udfs)
    publish_views.set_upstream(publish_new_tables)
