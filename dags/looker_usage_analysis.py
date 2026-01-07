from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
# Looker Usage Analysis

*Triage notes*

As long as the most recent DAG run is successful this job can be considered healthy.
In such case, past DAG failures can be ignored.

This DAG runs every quarter (1st day of February, May, August, November) and analyses the
Looker artifact usage using [Henry](https://github.com/looker-open-source/henry)
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 30),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

looker_client_id_prod = Secret(
    deploy_type="env",
    deploy_target="LOOKER_CLIENT_ID",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_id_prod",
)
looker_client_secret_prod = Secret(
    deploy_type="env",
    deploy_target="LOOKER_CLIENT_SECRET",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_secret_prod",
)
looker_instance_uri = "https://mozilla.cloud.looker.com"


with DAG(
    "looker_usage_analysis",
    doc_md=DOCS,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="0 0 1 2,5,8,11 *",
    tags=tags,
) as dag:
    airflow_gke_prod_kwargs = {
        "gcp_conn_id": "google_cloud_airflow_gke",
        "project_id": "moz-fx-data-airflow-gke-prod",
        "location": "us-west1",
        "cluster_name": "workloads-prod-v1",
    }

    analyze_explores = GKEPodOperator(
        task_id="analyze_explores",
        arguments=[
            "python",
            "-m",
            "looker_utils.main",
            "analyze",
            "--destination_table",
            "moz-fx-data-shared-prod.monitoring_derived.looker_usage_explores_v1",
            "--date={{ ds }}",
            "explores",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/looker-utils:latest",
        env_vars={
            "LOOKER_INSTANCE_URI": looker_instance_uri,
        },
        secrets=[looker_client_id_prod, looker_client_secret_prod],
        **airflow_gke_prod_kwargs,
    )

    analyze_models = GKEPodOperator(
        task_id="analyze_models",
        arguments=[
            "python",
            "-m",
            "looker_utils.main",
            "analyze",
            "--destination_table",
            "moz-fx-data-shared-prod.monitoring_derived.looker_usage_models_v1",
            "--date={{ ds }}",
            "models",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/looker-utils:latest",
        env_vars={
            "LOOKER_INSTANCE_URI": looker_instance_uri,
        },
        secrets=[looker_client_id_prod, looker_client_secret_prod],
        **airflow_gke_prod_kwargs,
    )

    analyze_unused_explores = GKEPodOperator(
        task_id="analyze_unused_explores",
        arguments=[
            "python",
            "-m",
            "looker_utils.main",
            "analyze",
            "--destination_table",
            "moz-fx-data-shared-prod.monitoring_derived.looker_usage_unused_explores_v1",
            "--date={{ ds }}",
            "unused-explores",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/looker-utils:latest",
        env_vars={
            "LOOKER_INSTANCE_URI": looker_instance_uri,
        },
        secrets=[looker_client_id_prod, looker_client_secret_prod],
        **airflow_gke_prod_kwargs,
    )
