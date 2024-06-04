"""
DAG to test out the monte_carlo Airflow integration.

This is just a POC. Ignore in Airflow triage
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow_mcd.operators import SimpleCircuitBreakerOperator

from utils.gcp import bigquery_etl_query
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3, Tag.Triage.no_triage]

with DAG(
    "monte_carlo_poc",
    default_args=default_args,
    schedule_interval="30 3 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    latest_versions = bigquery_etl_query(
        reattach_on_restart=True,
        task_id="latest_versions",
        destination_table="latest_versions_v1",
        dataset_id="telemetry_derived",
        sql_file_path="sql/data-observability-dev/telemetry_derived/latest_versions_v1/query.sql",
        project_id="data-observability-dev",
        date_partition_parameter=None,
        arguments=("--replace",),
    )

    monte_carlo__telemetry_derived__latest_versions_v1 = SimpleCircuitBreakerOperator(
        task_id="monte_carlo__telemetry_derived__latest_versions_v1",
        mcd_session_conn_id="monte_carlo_default_session_id",
        rule_uuid="438a4215-ab4a-40f0-9c34-64346fb5c486",
    )

    latest_versions >> monte_carlo__telemetry_derived__latest_versions_v1

    monte_carlo__fenix_derived__metrics_clients_last_seen_v1 = (
        SimpleCircuitBreakerOperator(
            task_id="monte_carlo__fenix_derived__metrics_clients_last_seen_v1",
            mcd_session_conn_id="monte_carlo_default_session_id",
            rule_uuid="3f9bd6be-e330-446c-a045-9394633b2c31",
        )
    )

    monte_carlo__fenix_derived__metrics_clients_last_seen_v1 = (
        SimpleCircuitBreakerOperator(
            task_id="monte_carlo__fenix_derived__firefox_android_anonymised_v1",
            mcd_session_conn_id="monte_carlo_default_session_id",
            rule_uuid="eb9c26c5-604f-4c71-b7bf-df3d818c55f4",
        )
    )
