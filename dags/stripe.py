from airflow import DAG
from datetime import timedelta, datetime
from utils.gcp import gke_command

default_args = {
    "owner": "dthorn@mozilla.com",
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 11),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG("stripe", default_args=default_args, schedule_interval="@daily") as dag:
    gke_command(
        task_id="stripe_import_events",
        command=[
            "bqetl",
            "stripe",
            "import",
            "--date={{ ds }}",
            "--api-key={{ var.value.stripe_api_key }}",
            "--resource=Event",
            "--table=moz-fx-data-shared-prod.stripe_external.events_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )
