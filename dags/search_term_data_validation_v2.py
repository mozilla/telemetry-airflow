"""
See [search-term-data-validation-v2 in the docker-etl repository](https://github.com/mozilla/docker-etl/blob/main/jobs/search-term-data-validation-v2).

This job populates a table for evaluating whether our recorded search terms
(candidate search volume for being sanitized and stored) are changing in ways
that might invalidate assumptions on which we've built our sanitization model.

This DAG is low priority.
"""

from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import gke_command
from utils.tags import Tag

default_args = {
    "owner": "ctroy@mozilla.com",
    "email": ["ctroy@mozilla.com", "wstuckey@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 5),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

daily_at_8AM = "0 8 * * *"
with DAG(
    "search_term_data_validation_v2",
    default_args=default_args,
    schedule_interval=daily_at_8AM,
    doc_md=__doc__,
    tags=tags,
) as dag:
    search_term_data_validation = gke_command(
        task_id="search_term_data_validation_v2",
        command=[
            'echo "HELLO WORLD!"',
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/search-alert_docker_etl:latest",
        dag=dag,
    )
