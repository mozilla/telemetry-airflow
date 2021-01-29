from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "frank@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2018, 12, 17),
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "release_telemetry_aggregates",
    default_args=default_args,
    schedule_interval="@daily",
)

# See mozaggregator_prerelease and mozaggregator_mobile for functional
# implementations using dataproc operator. This is not implemented due to the
# migration to GCP and https://bugzilla.mozilla.org/show_bug.cgi?id=1517018
release_telemetry_aggregate_view = DummyOperator(
    task_id="release_telemetry_aggregate_view",
    job_name="Release Telemetry Aggregate View",
    dag=dag,
)
