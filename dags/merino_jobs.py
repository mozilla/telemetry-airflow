import datetime
from copy import copy
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
        container_resources=k8s.V1ResourceRequirements(
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

polygon_prod_apikey_secret = Secret(
    deploy_type="env",
    deploy_target="MERINO_POLYGON__API_KEY",
    secret="airflow-gke-secrets",
    key="merino_polygon_secret__prod_api_key",
)

flightaware_prod_apikey_secret = Secret(
    deploy_type="env",
    deploy_target="MERINO_FLIGHTAWARE__API_KEY",
    secret="airflow-gke-secrets",
    key="merino_flightaware_secret__prod_api_key",
)

# The Secret defines how the DAG should get the credential.
# See https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/_modules/airflow/providers/cncf/kubernetes/secret.html#Secret
sports_prod_sportsdata_apikey_secret = Secret(
    # How is the secret deployed (usually as an `env`ironment variable)
    deploy_type="env",
    # What Environment variable should store this value (Talk to DAGENG about this value)
    # This value should match what the merino job is expecting.
    # In this case, we follow the `settings` model
    deploy_target="MERINO_PROVIDERS__SPORTS__SPORTSDATA__API_KEY",
    # Where is the secret stored in Kubernetes?
    secret="airflow-gke-secrets",
    # finally, what is the name of the secret in the storage (Talk to DAGENG about this value)
    key="merino_providers__sports__sportsdata_api_key",
)

# Sports will re-use the same Elastic search URL and API_KEY that wikipedia uses.
# BE SURE TO MODIFY BOTH SPORTS AND WIKIPEDIA IF THESE VALUES CHANGE!

# Values used by multiple DAGs
es_prod_connection = BaseHook.get_connection("merino_elasticsearch_prod")
es_staging_connection = BaseHook.get_connection("merino_elasticsearch_stage")


# Run weekly on Tuesdays at 5am UTC
with DAG(
    "merino_jobs",
    schedule_interval="0 5 * * 2",
    doc_md=DOCS,
    default_args=default_args,
    tags=tags,
) as dag:

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
                    "6600000",  # Estimate of the total number of documents in wikipedia index
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

    prepare_domain_metadata_prod_gcp_v2 = merino_job(
        "nav_suggestions_prepare_domain_metadata_prod_gcp_v2",
        arguments=[
            "navigational-suggestions",
            "prepare-domain-metadata",
            "--src-gcp-project",
            "moz-fx-data-shared-prod",
            "--dst-gcp-project",
            "moz-fx-merino-prod-5de4",
            "--dst-gcs-bucket",
            "merino-images-prod",
            "--dst-cdn-hostname",
            "prod-images.merino.prod.webservices.mozgcp.net",
            "--force-upload",
            "--write-xcom",
        ],
    )

    # polygon image ingestion task
    polygon_ingestion_prod = merino_job(
        name="polygon_ingestion_prod",
        arguments=[
            "polygon-ingestion",
            "ingest",
        ],
        secrets=[polygon_prod_apikey_secret],
    )

with DAG(
    "merino_flightaware_schedules",
    schedule_interval="0 */6 * * *",  # every 6 hours
    doc_md=DOCS,
    default_args=default_args,
    tags=tags,
) as dag:
    schedules_job = merino_job(
        name="fetch_flightaware_schedules_prod",
        arguments=["fetch_flights", "fetch-and-store"],
        secrets=[flightaware_prod_apikey_secret],
    )


# NOTE: Environment variable based settings for things like Elastic search URLs and credential sets, are NOT
# included in Airflow declarations. These values need to be explicitly specified.
sports_args = copy(default_args)
sports_args["email_on_failure"] = False
sports_args["email_on_retry"] = False
sports_args["email"] = ["jconlin+af@mozilla.com"]

# Sports Nightly
with DAG(
    "merino_sports_nightly",
    # This performs a data purge on accumulated sport events, so more frequent
    # calls can improve performance and limit costs.
    # This should be called at a minimum of once per day, around Midnight ET.
    # (Offsetting by 2 minutes to prevent potential overlap with `update`.)
    schedule_interval="2 */6 * * *",
    doc_md=DOCS,
    default_args=sports_args,
    tags=tags,
) as dag:
    es_secret = elasticsearch_prod_apikey_secret or elasticsearch_stage_apikey_secret
    # Note, assigning this to a value is probably not required, but may
    # make debugging easier.
    sports_nightly_job = merino_job(
        name="sports_nightly_job",
        env_vars={
            "MERINO_PROVIDERS__SPORTS__ES__DSN": es_prod_connection.host
            or es_staging_connection.host,
        },
        arguments=["fetch_sports", "nightly"],
        # NOTE: ALL secrets must be passed in explicitly
        secrets=[sports_prod_sportsdata_apikey_secret, es_secret],
    )

# Sports Update
with DAG(
    "merino_sports_update",
    # This updates current and pending sport events for the window
    # current date ±7 days. Called every 5 minutes.
    schedule_interval="*/5 * * * *",
    doc_md=DOCS,
    default_args=sports_args,
    tags=tags,
) as dag:
    es_secret = elasticsearch_prod_apikey_secret or elasticsearch_stage_apikey_secret
    sports_update_job = merino_job(
        name="sports_update_job",
        env_vars={
            "MERINO_PROVIDERS__SPORTS__ES__DSN": es_prod_connection.host
            or es_staging_connection.host,
        },
        arguments=["fetch_sports", "update"],
        # NOTE: ALL secrets must be passed in explicitly
        secrets=[sports_prod_sportsdata_apikey_secret, es_secret],
    )
