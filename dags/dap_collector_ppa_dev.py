from datetime import datetime

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### PPA Dev DAP Collector

#### Description

Runs a Docker image that collects PPA Dev Environment data from a DAP (Distributed Aggregation Protocol) leader and stores it in BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/dap-collector-ppa-dev)

This DAG requires following variables to be defined in Airflow:
* dap_ppa_dev_auth_token
* dap_ppa_dev_hpke_private_key
* dap_ppa_dev_task_config_url
* dap_ppa_dev_ad_config_url

This job is under active development, occasional failures are expected.

#### Owner

bbirdsong@mozilla.com
"""

default_args = {
    "owner": "bbirdsong@mozilla.com",
    "email": ["ads-eng@mozilla.com", "bbirdsong@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 30),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

project_id = "moz-fx-ads-nonprod"
ad_table_id = "ppa_dev.measurements"
report_table_id = "ppa_dev.reports"

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]


with DAG(
    "dap_collector_ppa_dev",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="0 0 * * *",
    tags=tags,
    catchup=False,
) as dag:
    dap_collector = GKEPodOperator(
        task_id="dap_collector_ppa_dev",
        arguments=[
            "python",
            "dap_collector_ppa_dev/main.py",
            "--date={{ ts }}",
            "--auth-token={{ var.value.dap_ppa_dev_auth_token }}",
            "--hpke-private-key={{ var.value.dap_ppa_dev_hpke_private_key }}",
            "--task-config-url={{ var.value.dap_ppa_dev_task_config_url }}",
            "--ad-config-url={{ var.value.dap_ppa_dev_ad_config_url }}",
            "--project",
            project_id,
            "--ad-table-id",
            ad_table_id,
            "--report-table-id",
            report_table_id,
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/dap-collector-ppa-dev_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )
