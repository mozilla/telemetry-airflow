"""
Runs a Docker image that imports Quicksuggest suggestions
from Remote Settings to BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/quicksuggest2bq)
"""

from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command


default_args = {
    "owner": "aplacitelli@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 18),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

project_id = "TODO"
table_id = "TODO"

with DAG("quicksuggest2bq",
         default_args=default_args,
         doc_md=__doc__,
         schedule_interval="@daily") as dag:

    quicksuggest2bq = gke_command(
        task_id="quicksuggest2bq",
        command=[
            "python", "quicksuggest2bq/main.py",
            "--destination-project", project_id,
            "--destination-table-id", table_id,
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/quicksuggest2bq:latest",
        gcp_conn_id="google_cloud_derived_datasets",
        dag=dag,
        email=[
            "aplacitelli@mozilla.com",
        ],
    )
