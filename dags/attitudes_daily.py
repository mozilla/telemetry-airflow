import datetime

from airflow import models
from airflow.models import Variable
from utils.gcp import gke_command

default_args = {
    "owner": "ssuh@mozilla.com",
    "start_date": datetime.datetime(2019, 12, 16),
    "email": ["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=30),
}

dag_name = "attitudes_daily"

with models.DAG(
        dag_name,
        schedule_interval="0 1 * * *",
        default_args=default_args) as dag:
    surveygizmo_attitudes_daily_import = gke_command(
        task_id="surveygizmo_attitudes_daily_import",
        command=[
            "python",
            "templates/telemetry_derived/surveygizmo_daily_attitudes/import_responses.py",
            "--date",
            "{{ ds }}",
            "--survey_id",
            Variable.get("surveygizmo_daily_attitudes_survey_id"),
            "--sg_api_token",
            Variable.get("surveygizmo_api_token"),
            "--sg_api_secret",
            Variable.get("surveygizmo_api_secret"),
            "--destination_table",
            "moz-fx-data-shared-prod.telemetry_derived.survey_gizmo_daily_attitudes"
        ],
        docker_image="mozilla/bigquery-etl:latest")
