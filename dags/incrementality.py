import os
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### Incrementality Collector

#### Description

Runs a Docker image that collects Incrementality data from a DAP (Distributed Aggregation Protocol) leader and stores it in BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/ads-incrementality-dap-collector)

This DAG requires following secrets to be defined in Airflow:
* dap_ads_incr_auth_token_dev
* dap_ads_incr_auth_token_prod
* dap_ads_incr_hpke_private_key_dev
* dap_ads_incr_hpke_private_key_prod

The following variables are defined in the Airflow:
* ads_incr_job_project_id
* ads_incr_job_config_bucket

This job is under active development, occasional failures are expected.

#### Owner
* gleonard@mozilla.com
* mlifshin@mozilla.com
"""

default_args = {
    "owner": "gleonard@mozilla.com",
    "email": [ "gleonard@mozilla.com", "mlifshin@mozilla.com"],  ## TODO Add "ads-eng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

deploy_env = os.environ.get("DEPLOY_ENVIRONMENT", "dev")

hpke_private_key = Secret(
    deploy_type="env",
    deploy_target="HPKE_PRIVATE_KEY",
    secret="airflow-gke-secrets",
    key="dap_ads_incr_hpke_private_key_" + deploy_env,
)

auth_token = Secret(
    deploy_type="env",
    deploy_target="AUTH_TOKEN",
    secret="airflow-gke-secrets",
    key="dap_ads_incr_auth_token_" + deploy_env,
)

with DAG(
    "dap_incrementality",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="15 0 * * *",
    tags=tags,
    catchup=False,
) as dag:
    dap_collector = GKEPodOperator(
        task_id="dap_incrementality",
        arguments=[
            "python",
            "ads_incrementality_dap_collector/main.py",
            "--job_config_gcp_project={{ var.value.ads_incr_job_project_id }}",
            "--job_config_bucket={{ var.value.ads_incr_job_config_bucket }}",
            "--process_date={{ ds }}"
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/ads_incrementality_dap_collector:latest",
        secrets=[
            hpke_private_key,
            auth_token,
        ],
    )
