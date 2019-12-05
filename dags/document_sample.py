from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 4),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("document_sample", default_args=default_args, schedule_interval="0 1 * * *")

document_sample_nonprod_v1 = bigquery_etl_query(
    destination_table="document_sample_nonprod_v1",
    dataset_id="monitoring",
    project_id="moz-fx-data-shared-prod",
    dag=dag,
)
