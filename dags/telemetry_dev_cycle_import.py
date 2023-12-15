"""
Runs a Docker image that imports different metrics from different APIs to build 
the Telemetry Dev Cycle Dashboard in Looker (link to be added here)

See the [`telemetry_dev_cycle`](https://github.com/mozilla/docker-etl/tree/main/jobs/telemetry-dev-cylce)
docker image defined in `docker-etl`.
"""

from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command
from utils.tags import Tag 

default_args = {
    "owner": "leli@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 15),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

project_id = "moz-fx-data-shared-prod"
data_set_id = "telemetry_dev_cycle_derived"

tags = [Tag.ImpactTier.tier_3]

with DAG(
    "telemetry_dev_cycle_import",
    default_args=default_args,
    doc_md=__doc__,
    schedule_interval="0 18 * * *",
    tags=tags,
) as dag:
    
    telemetry_dev_cycle_import = gke_command(
        task_id="telemetry_dev_cycle",
        command=[
            "python", "telemetry_dev_cycle/main.py",
            "--bq_project_id", project_id,
            "--bq_dataset_id", data_set_id,
            "--run_glean",
            "--run-telemetry",
            "--run_experiments"
        ]
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/telemetry-dev-cycle_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        email=[
            "leli@mozilla.com",
        ]
    )