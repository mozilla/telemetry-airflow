"""
Initialize query artifacts.

This DAG is triggered by bqetl_artifact_deployment and runs `bqetl query initialize`
for each project.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from kubernetes.client import models as k8s

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

tags = [Tag.ImpactTier.tier_1]

params = {
    "generate_sql": Param(
        default=False,
        type="boolean",
        description="Run SQL generation before initialize task",
    ),
}

# renders generate sql command if params.generate_sql is true, else empty string
generate_sql_cmd_template = (
    "{{ 'script/bqetl generate all --ignore derived_view_schemas --use-cloud-function=false && ' "
    "if params.generate_sql else '' }}"
)

generate_sql_container_resources = k8s.V1ResourceRequirements(
    requests={
        "memory": "{{ '6Gi' if params.generate_sql else '2Gi' }}",
        "cpu": "{{ '4' if params.generate_sql else '1' }}",
    },
)

with DAG(
    "bqetl_artifact_initialize",
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    schedule_interval=None,
    doc_md=__doc__,
    tags=tags,
    params=params,
) as dag:
    docker_image = (
        "us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest"
    )

    GKEPodOperator(
        task_id="query_initialize",
        cmds=["bash", "-x", "-c"],
        arguments=[
            generate_sql_cmd_template
            + "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-shared-prod --project-id=moz-fx-data-experiments --project-id=moz-fx-data-marketing-prod --project-id=moz-fx-data-bq-people"
        ],
        image=docker_image,
        container_resources=generate_sql_container_resources,
    )
