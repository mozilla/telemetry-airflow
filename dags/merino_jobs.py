import datetime
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
    Merino Jobs

    Dag for orchestrating jobs that build datasets that are used in Merino.
    The jobs are run via the GKEPodOperator
"""


def merino_job(
    name: str, arguments: List[str], env_vars: Optional[Dict[str, Any]] = None
):
    default_env_vars = {"MERINO_ENV": "production"}

    if env_vars is None:
        env_vars = {}

    return GKEPodOperator(
        task_id=name,
        name=name,
        image="mozilla/merino-py:latest",
        project_id="moz-fx-data-airflow-gke-prod",
        gcp_conn_id="google_cloud_airflow_gke",
        cluster_name="workloads-prod-v1",
        location="us-west1",
        cmds=["python", "-m", "merino.jobs.cli"],
        arguments=arguments,
        env_vars=default_env_vars | env_vars,
        email=[
            "wstuckey@mozilla.com",
        ],
    )


default_args = {
    "owner": "wstuckey@mozilla.com",
    "start_date": datetime.datetime(2023, 2, 1),
    "email": ["wstuckey@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

# Low priority, no triage
tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

# Run weekly on Tuesdays at 5am UTC
with DAG(
    "merino_jobs",
    schedule_interval="0 5 * * 2",
    doc_md=DOCS,
    default_args=default_args,
    tags=tags,
) as dag:
    es_prod_connection = BaseHook.get_connection("merino_elasticsearch_prod")
    es_staging_connection = BaseHook.get_connection("merino_elasticsearch_stage")

    wikipedia_indexer_copy_export = merino_job(
        "wikipedia_indexer_copy_export",
        arguments=[
            "wikipedia-indexer",
            "copy-export",
            "--gcs-path",
            "moz-fx-data-prod-external-data/contextual-services/merino-jobs/wikipedia-exports",
            "--gcp-project",
            "moz-fx-data-shared-prod",
        ],
    )

    wikipedia_indexer_build_index_for_staging = merino_job(
        "wikipedia_indexer_build_index_staging",
        arguments=[
            "wikipedia-indexer",
            "index",
            "--version",
            "v1",
            "--total-docs",
            "6600000",  # Estimate of the total number of documents in wikipedia index
            "--elasticsearch-url",
            str(es_staging_connection.host),
            "--gcs-path",
            "moz-fx-data-prod-external-data/contextual-services/merino-jobs/wikipedia-exports",
            "--gcp-project",
            "moz-fx-data-shared-prod",
        ],
        env_vars={
            # Using the API key in the argument list leaks the sensitive data into the airflow UI.
            "MERINO_JOBS__WIKIPEDIA_INDEXER__ES_API_KEY": es_staging_connection.password,
        },
    )

    wikipedia_indexer_build_index_for_prod = merino_job(
        "wikipedia_indexer_build_index_production",
        arguments=[
            "wikipedia-indexer",
            "index",
            "--version",
            "v1",
            "--total-docs",
            "6600000",  # Estimate of the total number of documents in wikipedia index
            "--elasticsearch-url",
            str(es_prod_connection.host),
            "--gcs-path",
            "moz-fx-data-prod-external-data/contextual-services/merino-jobs/wikipedia-exports",
            "--gcp-project",
            "moz-fx-data-shared-prod",
        ],
        env_vars={
            # Using the API key in the argument list leaks the sensitive data into the airflow UI.
            "MERINO_JOBS__WIKIPEDIA_INDEXER__ES_API_KEY": es_prod_connection.password,
        },
    )

    wikipedia_indexer_copy_export >> [
        wikipedia_indexer_build_index_for_staging,
        wikipedia_indexer_build_index_for_prod,
    ]

    # Navigational suggestions task.
    prepare_domain_metadata_stage = merino_job(
        "nav_suggestions_prepare_domain_metadata_stage",
        arguments=[
            "navigational-suggestions",
            "prepare-domain-metadata",
            "--src-gcp-project",
            "moz-fx-data-shared-prod",
            "--dst-gcp-project",
            "moz-fx-merino-nonprod-ee93",
            "--dst-gcs-bucket",
            "merino-images-stagepy",
        ],
    )

    prepare_domain_metadata_prod = merino_job(
        "nav_suggestions_prepare_domain_metadata_prod",
        arguments=[
            "navigational-suggestions",
            "prepare-domain-metadata",
            "--src-gcp-project",
            "moz-fx-data-shared-prod",
            "--dst-gcp-project",
            "moz-fx-merino-prod-1c2f ",
            "--dst-gcs-bucket",
            "merino-images-prodpy",
        ],
    )

    on_domain_success = EmailOperator(
        task_id="email_on_domain_success",
        to="wstuckey@mozilla.com",
        subject="Navigational Suggestions Domain Metadata job successful",
        html_content="""
        Job completed. Download the new top picks json file on GCS.
        """,
    )

    [prepare_domain_metadata_stage, prepare_domain_metadata_prod] >> on_domain_success
