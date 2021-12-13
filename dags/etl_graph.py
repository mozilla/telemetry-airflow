"""
See [etl-graph in the docker-etl repository]
(https://github.com/mozilla/docker-etl/blob/main/jobs/etl-graph).

This DAG is low priority, as it powers a stand-alone visualization internal
to the data platform.
"""

from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "email": ["amiyaguchi@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 1),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

with DAG("etl_graph", default_args=default_args, schedule_interval="0 2 * * *", doc_md=__doc__, tags=tags,) as dag:
    etl_graph = GKEPodOperator(
        task_id="etl_graph",
        name="etl_graph",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/etl-graph_docker_etl:latest",
        dag=dag,
    )
