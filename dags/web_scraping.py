import datetime

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator

docs = """
### web_scraping

Scrapes a few websites on a weekly basis &
loads data to GCS

Owner: lmcfall@mozilla.com
"""

default_args = {
    "owner": "lmcfall@mozilla.com",
    "start_date": datetime.datetime(2025, 9, 1, 0, 0),
    "end_date": None,
    "email": ["lmcfall@mozilla.com", "telemetry-alerts@mozilla.com"],
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
    schedule_interval="0 15 * * 2",
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
