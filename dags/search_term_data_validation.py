"""
See [search-term-data-validation in the docker-etl repository]
(https://github.com/mozilla/docker-etl/blob/main/jobs/seawrch-term-data-validation.

This job populates a table for evaluating whether our recorded search terms
(candidate search volume for being sanitized and stored) are changing in ways
that might invalidate assumptions on which we've built our sanitization model.

This DAG is low priority.
"""

from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import gke_command
from airflow.sensors.external_task import ExternalTaskSensor
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "ctroy@mozilla.com",
    "email": ["ctroy@mozilla.com", "wstuckey@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 22),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

daily_at_8AM = "0 8 * * *"
with DAG("search_term_data_validation", default_args=default_args, schedule_interval=daily_at_8AM, doc_md=__doc__, tags=tags, ) as dag:
    search_term_data_validation = gke_command(
        task_id="search_term_data_validation",
        command=[
            "python", "data_validation_job.py",
            "--data_validation_origin",
            "moz-fx-data-shared-prod.search_terms.sanitization_job_data_validation_metrics",
            "--data_validation_reporting_destination",
            "moz-fx-data-shared-prod.search_terms_derived.search_term_data_validation_reports_v1"
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/search-term-data-validation_docker_etl:latest",
        dag=dag,
    )

