"""Test DAG for GKEPodOperator."""

from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "benwu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 19),
    "email_on_failure": True,
    "retries": 0,
    "email": "benwu@mozilla.com",
}

with DAG(
    "test_k8s",
    default_args=default_args,
    doc_md=__doc__,
    schedule_interval=None,
    tags=[Tag.ImpactTier.tier_3],
) as dag:
    hello = GKEPodOperator(
        task_id="success",
        image="bash:latest",
        arguments=["bash", "-c", "for i in $(seq 1 30); do echo $i; done"],
    )

    failing_task = GKEPodOperator(
        task_id="failure",
        image="bash:latest",
        arguments=["bash", "-c", "for i in $(seq 1 30); do echo $i; done && exit 1"],
    )
