"""
DAG to test out the BigEye Airflow integration.

This is just a POC. Ignore in Airflow triage
"""

from datetime import datetime, timedelta

from airflow import DAG
from bigeye_airflow.operators.run_metrics_operator import RunMetricsOperator

from utils.gcp import bigquery_etl_query
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 21),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3, Tag.Triage.no_triage]

with DAG(
    "bigeye_poc",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    latest_versions = bigquery_etl_query(
        reattach_on_restart=True,
        task_id="latest_versions",
        destination_table="latest_versions_v1",
        dataset_id="telemetry_derived",
        sql_file_path="sql/data-observability-dev/telemetry/latest_versions/query.sql",
        project_id="data-observability-dev",
        date_partition_parameter=None,
        arguments=("--replace",),
        dag=dag,
    )

    bigeye__telemetry_derived__latest_versions_v1 = RunMetricsOperator(
        task_id="bigeye__telemetry_derived__latest_versions_v1",
        connection_id="bigeye_connection",
        warehouse_id=1817,
        schema_name="data-observability-dev.telemetry_derived",
        table_name="latest_versions_v1",
        circuit_breaker_mode=True,
        dag=dag,
    )

    latest_versions >> bigeye__telemetry_derived__latest_versions_v1

    bigeye__fenix_derived__metrics_clients_last_seen_v1 = RunMetricsOperator(
        task_id="bigeye__fenix_derived__metrics_clients_last_seen_v1",
        connection_id="bigeye_connection",
        warehouse_id=1817,
        schema_name="data-observability-dev.fenix_derived",
        table_name="metrics_clients_last_seen_v1",
        circuit_breaker_mode=True,
        dag=dag,
    )

    bigeye__fenix_derived__firefox_android_anonymised_v1 = RunMetricsOperator(
        task_id="bigeye__fenix_derived__firefox_android_anonymised_v1",
        connection_id="bigeye_connection",
        warehouse_id=1817,
        schema_name="data-observability-dev.fenix_derived",
        table_name="firefox_android_anonymised_v1",
        circuit_breaker_mode=True,
        dag=dag,
    )
