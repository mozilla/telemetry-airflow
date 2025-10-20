import datetime
from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator

docs = """
### release_scraping

Reads Chrome release notes and loads it to GCS

Owner: kwindau@mozilla.com
"""

default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 9, 1, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/telemetry-airflow"]
SERVER = "moz-fx-data-airflow-prod-88e0"
IMAGE_NAME = "release_scraping_docker_etl:latest"

with DAG(
    "release_scraping",
    default_args=default_args,
    schedule_interval="0 15 2 * *",
    doc_md=docs,
    tags=tags,
) as dag:
    read_release_data = GKEPodOperator(
        task_id="read_release_data",
        arguments=[
            "python",
            "release-scraping/main.py",
            "--date",
            "{{ ds }}",
        ],
        image=f"gcr.io/{SERVER}/{IMAGE_NAME}",
        gcp_conn_id="google_cloud_airflow_gke",
    )
