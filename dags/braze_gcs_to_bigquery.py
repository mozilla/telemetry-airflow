"""Braze exports data via currents to Google Cloud Storage. This airflow job imports the data from there into BigQuery tables for future use."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from utils.tags import Tag

doc_md_DAG = """
# braze_gcs_to_bigquery
Build from telemetry-airflow repo, [dags/braze_gcs_to_bigquery.py](https://github.com/mozilla/telemetry-airflow/blob/main/dags/braze_gcs_to_bigquery.py)

### description
DAG for importing Braze Current Data from Google Cloud Storage buckets to BigQuery.

Braze exports data via currents to Google Cloud Storage. This airflow job imports the data from there into BigQuery tables for future use.
Ticket: [DENG-3411](https://mozilla-hub.atlassian.net/browse/DENG-3411)

### Owner
leli@mozilla.com

### Tags
- impact/tier_2
- repo/telemetry_airflow
"""

tags = [Tag.ImpactTier.tier_2, Tag.Repo.airflow]

default_args = {
    "owner": "leli@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 15),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
    "email": ["leli@mozilla.com"],
}

project_id = "moz-fx-data-shared-prod"
dataset_id = "braze_external"

with DAG(
    "bqetl_braze_currents_to_bigquery",
    doc_md=doc_md_DAG,
    tags=tags,
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    hard_bounces = GCSToBigQueryOperator(
        task_id="hard_bounce_2bq",
        bucket="moz-fx-data-marketing-prod-braze-firefox",
        source_objects=[
            "currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071/event_type=users.messages.email.Bounce*"
        ],
        destination_project_dataset_table=f"{project_id}.{dataset_id}.hard_bounces",
        write_disposition="WRITE_TRUNCATE",
    )
