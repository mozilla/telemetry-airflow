from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "email": ["amiyaguchi@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 19),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("etl_graph", default_args=default_args, schedule_interval="0 2 * * *") as dag:
    etl_graph = GKEPodOperator(
        task_id="etl_graph",
        name="etl_graph",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/etl-graph_docker_etl:latest",
        dag=dag,
    )
