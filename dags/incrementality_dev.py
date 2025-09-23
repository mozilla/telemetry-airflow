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

This DAG requires following variables to be defined in Airflow:
* dap_ppa_prod_auth_token
* dap_ppa_prod_hpke_private_key
* TBD

This job is under active development, occasional failures are expected.

#### Owner

gleonard@mozilla.com
mlifshin@mozilla.com
"""

default_args = {
    "owner": "gleonard@mozilla.com",
    "email": ["ads-eng@mozilla.com", "gleonard@mozilla.com", "mlifshin@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0, # TODO GLE: what to do about retries
}

job_project_id = "moz-fx-dev-gleonard-ads"
job_config_bucket = "ads-gleonard-etl-config"

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

hpke_private_key = Secret(
    deploy_type="env",
    deploy_target="HPKE_PRIVATE_KEY",
    secret="airflow-gke-secrets",
    key="DAP_PPA_DEV_HPKE_PRIVATE_KEY",
)

auth_token = Secret(
    deploy_type="env",
    deploy_target="HPKE_TOKEN",  # TODO need to rename to auth_token in docker etl.
    secret="airflow-gke-secrets",
    key="DAP_PPA_DEV_AUTH_TOKEN",
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
            "ads-incrementality-dap-collector/main.py",
            "--batch_start={{ data_interval_end.at(0) | ts }}",
            "--gcp_project",
            job_project_id,
            "--job_config_bucket",
            job_config_bucket,
        ],

        image="us-central1-docker.pkg.dev/moz-fx-dev-gleonard-ads/incrementality/ads_incrementality_dap_collector:v2",
        gcp_conn_id='google_cloud_gke_sandbox',
        secrets=[
            hpke_private_key,
            auth_token,
        ],
    )
