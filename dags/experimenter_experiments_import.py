from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime
from utils.gcp import gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com", "tdsmith@mozilla.com",],
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 9),
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG("experimenter_experiments_import", default_args=default_args, schedule_interval="*/10 * * * *") as dag:

    experimenter_experiments_import = gke_command(
        task_id="experimenter_experiments_import",
        command=[
            "python",
            "sql/moz-fx-data-experiments/monitoring/experimenter_experiments_v1/query.py",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com", "tdsmith@mozilla.com"],
    )
