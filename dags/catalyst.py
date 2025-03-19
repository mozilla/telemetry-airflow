"""
DAG to schedule generation of performance reports for recently completed nimbus experiments.

See the [jetstream repository](https://github.com/mozilla/catalyst).

"""

from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "dpalmeiro@mozilla.com",
    "email": [
        "dpalmeiro@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 18),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG(
    "catalyst",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    # Built from repo https://github.com/mozilla/catalyst
    jetstream_image = "gcr.io/moz-fx-data-experiments/catalyst:latest"

    jetstream_run = GKEPodOperator(
        task_id="catalyst_run",
        name="catalyst_run",
        image=jetstream_image,
        email=default_args["email"],
        dag=dag,
    )
