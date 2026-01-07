from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### PPA Prod DAP Collector

#### Description

Runs a Docker image that collects PPA Prod Environment data from a DAP (Distributed Aggregation Protocol) leader and stores it in BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/dap-collector-ppa-prod)

This DAG requires following variables to be defined in Airflow:
* dap_ppa_prod_auth_token
* dap_ppa_prod_hpke_private_key
* dap_ppa_prod_task_config_url
* dap_ppa_prod_ad_config_url

This job is under active development, occasional failures are expected.

#### Owner

bbirdsong@mozilla.com
"""

default_args = {
    "owner": "bbirdsong@mozilla.com",
    "email": ["ads-eng@mozilla.com", "bbirdsong@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 26),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

project_id = "moz-fx-ads-prod"
ad_table_id = "ppa.measurements"
report_table_id = "ppa.reports"

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

hpke_private_key = Secret(
    deploy_type="env",
    deploy_target="HPKE_PRIVATE_KEY",
    secret="airflow-gke-secrets",
    key="DAP_PPA_PROD_HPKE_PRIVATE_KEY",
)

auth_token = Secret(
    deploy_type="env",
    deploy_target="AUTH_TOKEN",
    secret="airflow-gke-secrets",
    key="DAP_PPA_PROD_AUTH_TOKEN",
)

with DAG(
    "dap_collector_ppa_prod",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="15 0 * * *",
    tags=tags,
    catchup=False,
) as dag:
    dap_collector = GKEPodOperator(
        task_id="dap_collector_ppa_prod",
        arguments=[
            "python",
            "dap_collector_ppa_prod/main.py",
            "--date={{ data_interval_end.at(0) | ts }}",
            "--task-config-url={{ var.value.dap_ppa_prod_task_config_url }}",
            "--ad-config-url={{ var.value.dap_ppa_prod_ad_config_url }}",
            "--project",
            project_id,
            "--ad-table-id",
            ad_table_id,
            "--report-table-id",
            report_table_id,
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/dap-collector-ppa-prod:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        secrets=[
            hpke_private_key,
            auth_token,
        ],
    )
