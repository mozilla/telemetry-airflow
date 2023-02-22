import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.hooks.base import BaseHook

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
    Merino Jobs

    Dag for orchestrating jobs that build datasets that are used in Merino.
    The jobs are run via the GKEPodOperator 
"""

def merino_job(name: str, arguments: List[str], env_vars: Dict[str, Any] = {}):
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
        env_vars=env_vars,
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

# Run weekly on Mondays at 5am UTC
with DAG(
    "merino_jobs",
    schedule_interval="0 5 * * 1",
    doc_md=DOCS,
    default_args=default_args,
    tags=tags,
) as dag:

    conn = BaseHook.get_connection("merino_elasticsearch")

    wikipedia_indexer_copy_export = merino_job(
        "wikipedia_indexer_copy_export",
        arguments=[
            "wikipedia-indexer",
            "copy-export",
            "--gcs-path", "moz-fx-data-prod-external-data/contextual-services/merino-jobs/wikipedia-exports",
            "--gcp-project", "moz-fx-data-shared-prod",
        ],
        env_vars=dict(
            MERINO_ENV="production",
        ),
    )

    wikipedia_indexer_build_index = merino_job(
        "wikipedia_indexer_build_index",
        arguments=[
            "wikipedia-indexer",
            "index",
            "--elasticsearch-cloud-id", str(conn.host),
            "--elasticsearch-api-key", conn.password,
        ],
        env_vars=dict(
            MERINO_ENV="production",
        ),
    )



    wikipedia_indexer_copy_export >> wikipedia_indexer_build_index
