from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import gke_command
from utils.tags import Tag

DOCS = """
### DAP Collector

#### Description

Runs a Docker image that collects data from a DAP (Distributed Aggregation Protocol) leader and stores it in BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/dap-collector)

For more information on Privacy Preserving Measurement in Firefox see
https://bugzilla.mozilla.org/show_bug.cgi?id=1775035

This DAG requires following variables to be defined in Airflow:
* dap_auth_token
* dap_hpke_private_key

This job is under active development, occasional failures are expected.

#### Owner

sfriedberger@mozilla.com
"""

default_args = {
    "owner": "sfriedberger@mozilla.com",
    "email": ["akomarzewski@mozilla.com", "sfriedberger@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 8),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(hours=2),
}

project_id = "moz-fx-data-shared-prod"
table_id = "dap_collector_derived.aggregates_v1"

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

with DAG(
    "dap_collector",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="@daily",
    tags=tags,
) as dag:
    dap_collector = gke_command(
        task_id="dap_collector",
        command=[
            "python",
            "dap_collector/main.py",
            "--date={{ ds }}",
            "--auth-token={{ var.value.dap_auth_token }}",
            "--hpke-private-key={{ var.value.dap_hpke_private_key }}",
            "--project",
            project_id,
            "--table-id",
            table_id,
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/dap-collector_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )
