"""
DAG to schedule generation of results for partybal.

Partybal is an experimental service to visualize experiment results that have been
produced by [jetstream](https://github.com/mozilla/jetstream).
See https://github.com/mozilla/partybal

This DAG depends on experiment results being available for a certain date.
So if the [jetstream DAG](https://workflow.telemetry.mozilla.org/tree?dag_id=jetstream)
does not successfully complete running, then the tasks in this DAG will fail as well.

The DAG is scheduled to run every three hours to pick up experiment results from manually
triggered analysis runs quickly.
"""

from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
        "mwilliams@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 21),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "partybal",
    default_args=default_args,
    schedule_interval="0 */3 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    # Built from repo https://github.com/mozilla/partybal
    partybal_image = "gcr.io/moz-fx-data-experiments/partybal:latest"

    partybal = GKEPodOperator(
        task_id="partybal",
        name="partybal",
        image=partybal_image,
        email=[
            "ascholtz@mozilla.com",
            "mwilliams@mozilla.com",
        ],
        dag=dag,
    )
