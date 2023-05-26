from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import gke_command
from utils.tags import Tag

DOCS = """
### Influx DB to Bigquery

#### Description

Runs a Docker image that collects data from Influx DB (Database for contile related data) and stores it in BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/influxdb-to-bigquery)

For more information on 
https://mozilla-hub.atlassian.net/browse/RS-683

This DAG requires following variables to be defined in Airflow:
* influxdb_host_url
* influxdb_username
* influxdb_password

This job is under active development, occasional failures are expected.

#### Owner

akommasani@mozilla.com
"""

default_args = {
    "owner": "akommasani@mozilla.com",
    "email": ["akommasani@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 20),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(hours=2),
}


tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

with DAG(
    "influxdb_to_bigquery",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="@daily",
    tags=tags,
) as dag:
    contile_adm_request = gke_command(
        task_id="contile_adm_request",
        command=[
            "python",
            "influxdb_to_bigquery/main.py",
            "--date={{ ds }}",
            "--influxdb_host={{ var.value.influxdb_host }}",
            "--influxdb_username={{ var.value.influxdb_username }}",
            "--influxdb_password={{ var.value.influxdb_password }}",
            '--influxdb_measurement="svcops"."autogen"."contile.tiles.adm.request" ',
            "--bq_project_id=moz-fx-data-shared-prod",
            "--bq_dataset_id=telemetry_derived",
            "--bq_table_id=contile_tiles_adm_request",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/influxdb-to-bigquery_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )
    adm_response_tiles_count = gke_command(
        task_id="adm_response_tiles_count",
        command=[
            "python",
            "influxdb_to_bigquery/main.py",
            "--date={{ ds }}",
            "--influxdb_host={{ var.value.influxdb_host }}",
            "--influxdb_username={{ var.value.influxdb_username }}",
            "--influxdb_password={{ var.value.influxdb_password }}",
            '--influxdb_measurement="svcops"."autogen"."contile.tiles.adm.response.tiles_count" ',
            "--bq_project_id=moz-fx-data-shared-prod",
            "--bq_dataset_id=telemetry_derived",
            "--bq_table_id=contile_tiles_adm_response_tiles_count",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/influxdb-to-bigquery_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )
    adm_empty_response = gke_command(
        task_id="adm_empty_response",
        command=[
            "python",
            "influxdb_to_bigquery/main.py",
            "--date={{ ds }}",
            "--influxdb_host={{ var.value.influxdb_host }}",
            "--influxdb_username={{ var.value.influxdb_username }}",
            "--influxdb_password={{ var.value.influxdb_password }}",
            '--influxdb_measurement="svcops"."autogen"."contile.filter.adm.empty_response" ',
            "--bq_project_id=moz-fx-data-shared-prod",
            "--bq_dataset_id=telemetry_derived",
            "--bq_table_id=contile_filter_adm_empty_response",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/influxdb-to-bigquery_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )

    contile_adm_request >> adm_response_tiles_count >> adm_empty_response
