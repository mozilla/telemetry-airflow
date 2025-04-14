import datetime

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator

docs = """
### extensions

Loads the table moz-fx-data-shared-prod.external_derived.chrome_extensions_v1

Note - if it fails, please alert the DAG owner, but do not re-run.

Owner: kwindau@mozilla.com
"""

default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 4, 13, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/telemetry-airflow"]
SERVER = "moz-fx-data-airflow-prod-88e0"
IMAGE_NAME = "extensions_docker_etl:latest"

with DAG(
    "extensions",
    default_args=default_args,
    schedule_interval="0 15 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    pull_extensions = GKEPodOperator(
        task_id="pull_extensions",
        arguments=[
            "python",
            "extensions/main.py",
            "--date",
            "{{ ds }}",
        ],
        image=f"gcr.io/{SERVER}/{IMAGE_NAME}",
        gcp_conn_id="google_cloud_airflow_gke",
    )
