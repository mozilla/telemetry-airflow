import os
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### NewTab Attribution Collector

#### Description

Runs a Docker image that collects NewTab Attribution data from a DAP (Distributed Aggregation Protocol) leader and stores it in BigQuery.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/ads-attribution-dap-collector)

This DAG requires following secrets to be defined in Airflow:
* dap_ads_attr_auth_token_dev
* dap_ads_attr_auth_token_prod
* dap_ads_attr_hpke_private_key_dev
* dap_ads_attr_hpke_private_key_prod

The following variables are defined in the Airflow:
* ads_attr_job_project_id
* ads_attr_job_config_bucket

This job is expected to be stable however occasional failures may occur.

1. If the log contains the text "Collection timed out for " then clear the task to rerun it.  Since rerunning may result 
in data duplication notify the ads-eng team via Slack #ads-team-support indicating that the task was rerun.

2. If the log contains the text "Verify start date is not more than 14 days ago" then a retry is not needed.  DAP 
aggressively deletes data and the data required for the task to complete has been deleted.  This should only be seen in
 historical tasks.

3. If the log contains the text "Collection failed for" reach out to the ads-eng team via Slack #ads-team-support 
for assistance with the investigation.


#### Owner
* gleonard@mozilla.com
* mlifshin@mozilla.com
"""

default_args = {
    "owner": "gleonard@mozilla.com",
    "email": ["ads-eng@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 23),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

deploy_env = os.environ.get("ENVIRONMENT", "dev")

hpke_private_key = Secret(
    deploy_type="env",
    deploy_target="DAP_PRIVATE_KEY",
    secret="airflow-gke-secrets",
    key="dap_ads_attr_hpke_private_key_" + deploy_env,
)

bearer_token = Secret(
    deploy_type="env",
    deploy_target="DAP_BEARER_TOKEN",
    secret="airflow-gke-secrets",
    key="dap_ads_attr_auth_token_" + deploy_env,
)

bq_project="moz-fx-data-shar-nonprod-efed"
if deploy_env == "prod":
    bq_project = "moz-fx-data-shared-prod"

with DAG(
    "ads_newtab_attribution",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="15 1 * * *",
    tags=tags,
    catchup=False,
) as dag:
    dap_collector = GKEPodOperator(
        task_id="ads_newtab_attribution",
        arguments=[
            "python",
            "-m"
            "ads_attribution_dap_collector.main",
            "--bq_project",
            bq_project,
            "--job_config_gcp_project={{ var.value.ads_attr_job_project_id }}",
            "--job_config_bucket={{ var.value.ads_attr_job_config_bucket }}",
            "--process_date={{ ds }}",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/ads-attribution-dap-collector:latest",
        secrets=[
            hpke_private_key,
            bearer_token,
        ],
    )
