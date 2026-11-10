"""
Testing permissions of the airflow service account.
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

SERVICE_ACCOUNT_NAME = None

SCRIPT = (
    Path(__file__).resolve().parent.parent / "jobs" / "metric_usage_access_check.py"
).read_text()

if any(marker in SCRIPT for marker in ("{{", "{%", "{#")):
    raise ValueError("metric_usage_access_check.py must not contain Jinja markers")

default_args = {
    "owner": "bewu@mozilla.com",
    "email": [],
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

tags = [Tag.ImpactTier.tier_3, Tag.Triage.no_triage]

looker_client_id = Secret(
    deploy_type="env",
    deploy_target="LOOKERSDK_CLIENT_ID",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_id_prod",
)
looker_client_secret = Secret(
    deploy_type="env",
    deploy_target="LOOKERSDK_CLIENT_SECRET",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_secret_prod",
)

with DAG(
    "metric_usage_access_check",
    default_args=default_args,
    doc_md=__doc__,
    start_date=datetime(2026, 11, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=tags,
    params={
        "billing_project": Param(
            "",
            type="string",
            description="Project the check's queries run in; empty for the account's own",
        ),
        "write_dataset": Param(
            "mozdata.analysis",
            type="string",
            description="Dataset to write and delete a scratch table and view in",
        ),
    },
) as dag:
    GKEPodOperator(
        task_id="check_access",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        arguments=["python", "-c", SCRIPT],
        env_vars={
            "BILLING_PROJECT": "{{ params.billing_project }}",
            "WRITE_DATASET": "{{ params.write_dataset }}",
        },
        secrets=[looker_client_id, looker_client_secret],
        service_account_name=SERVICE_ACCOUNT_NAME,
    )
