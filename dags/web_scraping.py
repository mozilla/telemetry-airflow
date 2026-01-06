import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker

from operators.gcp_container_operator import GKEPodOperator

docs = """
### web_scraping

Scrapes a few websites on a monthly basis &
loads data to GCS to be used in the monthly market intel bot report

Owner: lmcfall@mozilla.com
"""

default_args = {
    "owner": "lmcfall@mozilla.com",
    "start_date": datetime.datetime(2025, 9, 1, 0, 0),
    "end_date": None,
    "email": ["lmcfall@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/telemetry-airflow"]

with DAG(
    "web_scraping",
    default_args=default_args,
    schedule_interval="0 15 2 * *",
    doc_md=docs,
    tags=tags,
) as dag:
    read_release_data = GKEPodOperator(
        task_id="read_release_data",
        arguments=[
            "python",
            "release_scraping/main.py",
            "--date",
            "{{ ds }}",
        ],
        image=f"us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/release_scraping:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )

    # This marks a specific task in the downstream DAG so clears cascade there.
    run_bqetl_market_intel_bot = ExternalTaskMarker(
        task_id="run_bqetl_market_intel_bot",
        external_dag_id="bqetl_market_intel_bot",
        external_task_id="wait_for_read_release_data",
    )

    read_release_data >> run_bqetl_market_intel_bot
