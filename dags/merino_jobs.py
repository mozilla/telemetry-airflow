import datetime
from typing import Any

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s
from airflow.utils.task_group import TaskGroup


from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
    Merino Jobs

    Dag for orchestrating jobs that build datasets that are used in Merino.
    The jobs are run via the GKEPodOperator
"""

SUPPORTED_LANGUAGES = ["en", "fr", "de", "it", "pl"]

def merino_job(
    name: str, arguments: list[str], env_vars: dict[str, Any] | None = None, **kwargs
):
    default_env_vars = {"MERINO_ENV": "production"}
    if env_vars is None:
        env_vars = {}
    default_env_vars.update(env_vars)

    return GKEPodOperator(
        task_id=name,
        name=name,
        image="mozilla/merino-py:latest",
        image_pull_policy="Always",
        project_id="moz-fx-data-airflow-gke-prod",
        gcp_conn_id="google_cloud_airflow_gke",
        cluster_name="workloads-prod-v1",
        location="us-west1",
        cmds=["python", "-m", "merino.jobs.cli"],
        arguments=arguments,
        # Needed for the jobs increased amount of domain it has
        # to process.
        container_resources = k8s.V1ResourceRequirements(
            requests={"memory": "512Mi"},
        ),
        env_vars=default_env_vars,
        email=[
            "disco-team@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        **kwargs,
    )


default_args = {
    "owner": "disco-team@mozilla.com",
    "start_date": datetime.datetime(2023, 2, 1),
    "email": ["disco-team@mozilla.com", "wstuckey@mozilla.com"],
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

elasticsearch_stage_apikey_secret = Secret(
    deploy_type="env",
    deploy_target="MERINO_JOBS__WIKIPEDIA_INDEXER__ES_API_KEY",
    secret="airflow-gke-secrets",
    key="merino_elasticsearch_secret__stage_api_key",
)

elasticsearch_prod_apikey_secret = Secret(
    deploy_type="env",
    deploy_target="MERINO_JOBS__WIKIPEDIA_INDEXER__ES_API_KEY",
    secret="airflow-gke-secrets",
    key="merino_elasticsearch_secret__prod_api_key",
)

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

    for lang in SUPPORTED_LANGUAGES:
        with TaskGroup(group_id=f"wikipedia_indexer_{lang}"):
            copy_export_task = merino_job(
                name=f"wikipedia_indexer_copy_export_{lang}",
                arguments=[
                    "wikipedia-indexer",
                    "copy-export",
                    "--gcs-path",
                    "moz-fx-data-prod-external-data/contextual-services/merino-jobs/wikipedia-exports",
                    "--gcp-project",
                    "moz-fx-data-shared-prod",
                    "--language",
                    lang,
                ],
            )

            wiki_index_staging_task = merino_job(
                name=f"wikipedia_indexer_build_index_staging_{lang}",
                arguments=[
                    "wikipedia-indexer",
                    "index",
                    "--language",
                    lang,
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
                secrets=[elasticsearch_stage_apikey_secret],
            )

            wiki_index_prod_task = merino_job(
                name=f"wikipedia_indexer_build_index_prod_{lang}",
                arguments=[
                    "wikipedia-indexer",
                    "index",
                    "--language",
                    lang,
                    "--version",
                    "v1",
                    "--total-docs",
                    "6600000", # Estimate of the total number of documents in wikipedia index
                    "--elasticsearch-url",
                    str(es_prod_connection.host),
                    "--gcs-path",
                    "moz-fx-data-prod-external-data/contextual-services/merino-jobs/wikipedia-exports",
                    "--gcp-project",
                    "moz-fx-data-shared-prod",
                ],
                secrets=[elasticsearch_prod_apikey_secret],
            )

            copy_export_task >> [wiki_index_staging_task, wiki_index_prod_task]

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
            "--dst-cdn-hostname",
            "stagepy-images.merino.nonprod.cloudops.mozgcp.net",
            "--force-upload",
            "--write-xcom",
        ],
        do_xcom_push=True,
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
            "--dst-cdn-hostname",
            "merino-images.services.mozilla.com",
            "--force-upload",
            "--write-xcom",
        ],
        do_xcom_push=True,
    )

    on_domain_success = EmailOperator(
        task_id="email_on_domain_success",
        to=["disco-team@mozilla.com", "wstuckey@mozilla.com"],
        subject="Navigational Suggestions Domain Metadata job successful",
        html_content="""
        Job completed. Download the new top picks json file on GCS.

        Prod Top Pick JSON file: {{ task_instance.xcom_pull("nav_suggestions_prepare_domain_metadata_prod")["top_pick_url"]}}
        Stage Top Pick JSON file: {{ task_instance.xcom_pull("nav_suggestions_prepare_domain_metadata_stage")["top_pick_url"]}}

        Change Summary:
        {{ task_instance.xcom_pull("nav_suggestions_prepare_domain_metadata_prod")["diff"]}}
        """,
    )

    [prepare_domain_metadata_stage, prepare_domain_metadata_prod] >> on_domain_success

    # polygon image ingestion task
    polygon_ingestion_prod = merino_job(
        name="polygon_ingestion_prod",
        arguments=[
            "polygon-ingestion",
            "ingest",
        ],
        secrets=[], #TODO - populate with polygon secret
    )

