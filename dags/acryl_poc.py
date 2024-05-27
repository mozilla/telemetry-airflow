"""
DAG to test out the datahub Airflow integration.

This is just a POC. Ignore in Airflow triage
"""

from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag
from utils.gcp import bigquery_etl_query, bigquery_dq_check
from datahub_airflow_plugin.operators.datahub_assertion_operator import DataHubAssertionOperator

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
    "datahub_poc",
    default_args=default_args,
    schedule_interval="15 3 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    latest_versions = bigquery_etl_query(
        reattach_on_restart=True,
        task_id="latest_versions",
        destination_table="latest_versions_v1",
        dataset_id="telemetry_derived",
        sql_file_path=f"sql/data-observability-dev/telemetry/latest_versions/query.sql",
        project_id="data-observability-dev",
        date_partition_parameter=None,
        arguments=("--replace",),
        dag=dag,
    )

    datahub__telemetry_derived__latest_versions_v1 = DataHubAssertionOperator(
        task_id='datahub__telemetry_derived__latest_versions_v1',
        datahub_rest_conn_id='datahub_rest_default',
        urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,data-observability-dev.telemetry_derived.latest_versions_v1,PROD)",
        dag=dag
    )

    latest_versions >> datahub__telemetry_derived__latest_versions_v1

    datahub__fenix_derived__metrics_clients_last_seen_v1 = DataHubAssertionOperator(
        task_id='datahub__fenix_derived__metrics_clients_last_seen_v1',
        datahub_rest_conn_id='datahub_rest_default',
        urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,data-observability-dev.fenix_derived.metrics_clients_last_seen_v1,PROD)",
        dag=dag
    )

    datahub__fenix_derived__firefox_android_anonymised_v1 = DataHubAssertionOperator(
        task_id='datahub__fenix_derived__firefox_android_anonymised_v1',
        datahub_rest_conn_id='datahub_rest_default',
        urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,data-observability-dev.fenix_derived.firefox_android_anonymised_v1,PROD)",
        dag=dag
    )
