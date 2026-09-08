from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### data_classification

#### Description

Manually triggered DAG that labels BigQuery columns with a category from the
Mozilla data taxonomy. One row per column lands in
`moz-fx-data-shared-prod.data_governance_metadata_derived.column_classifications_v1`.

`targets` is a list of classification targets formatted as `project.dataset` or
`project.dataset.table`, one per line.

`profile` runs the data governance profiler over those targets. `lineage` and
`probes` then run their metadata jobs over the whole date partition the profiler
wrote.
All three write to interim `classification_*` tables. `classify` builds a prompt per column, calls Gemini,
and records the label alongside the evidence it was based on. Sample values go through Cloud DLP
before the model sees them.

Re-running is safe. Columns already classified with the same model are skipped,
so a retry redoes only what failed.

See `bigquery_etl/data_governance/classification/README.md` in bigquery-etl, and
https://mozilla-hub.atlassian.net/browse/DENG-11453.

#### Owner

* akomarzewski@mozilla.com
* cmorales@mozilla.com
"""

DOCKER_IMAGE = "us-docker.pkg.dev/moz-fx-data-artifacts-prod/private-bigquery-etl/private-bigquery-etl:latest"

datahub_gms_token = Secret(
    deploy_type="env",
    deploy_target="DATAHUB_GMS_TOKEN",
    secret="airflow-gke-restricted-secrets",
    key="bqetl_data_governance_metadata__datahub_gms_token",
)

params = {
    "targets": Param(
        default=["moz-fx-data-shared-prod.telemetry_derived"],
        description=(
            "Classification targets as project.dataset or project.dataset.table"
            " (one per line)"
        ),
        type="array",
        minItems=1,
    ),
    "date": Param(
        default="",
        description=(
            "Partition the first three tasks read and write, as YYYY-MM-DD."
            " Defaults to the run's logical date."
        ),
        type="string",
    ),
}

default_args = {
    "owner": "akomarzewski@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 24),
    "email": ["akomarzewski@mozilla.com", "cmorales@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

# The profiler samples rows and holds them in memory before loading them, so it
# gets more than the steps that only move metadata around.
profile_resources = k8s.V1ResourceRequirements(
    requests={"memory": "4Gi", "cpu": "2"},
)
default_resources = k8s.V1ResourceRequirements(
    requests={"memory": "2Gi", "cpu": "1"},
)

# The three upstream steps chain on this date: each reads the partition the one
# before it wrote. Rendering it once per task from the same expression keeps a
# pass that crosses midnight on one date.
DATE = "--date={{ params.date or ds }}"


with DAG(
    "data_classification",
    default_args=default_args,
    schedule=None,
    # The pipeline assumes one run at a time. Two runs over the same date share
    # a scratch table per dataset, and their writes to the classification tables
    # would interleave.
    max_active_runs=1,
    doc_md=docs,
    tags=tags,
    params=params,
    # Needed to pass the targets to the pod operator as a list, not a string.
    render_template_as_native_obj=True,
) as dag:
    profile = GKEPodOperator(
        task_id="profile",
        cmds=["script/bqetl", "data_governance", "profile", DATE],
        arguments="{{ params.targets }}",
        image=DOCKER_IMAGE,
        container_resources=profile_resources,
        execution_timeout=timedelta(hours=4),
        # Profiling a large dataset runs long enough that a scheduler restart is
        # plausible, and a second pod would write the same rows as the first.
        reattach_on_restart=True,
    )

    lineage = GKEPodOperator(
        task_id="lineage",
        cmds=["script/bqetl", "data_governance", "lineage", DATE],
        image=DOCKER_IMAGE,
        container_resources=default_resources,
        execution_timeout=timedelta(hours=1),
        secrets=[datahub_gms_token],
    )

    probes = GKEPodOperator(
        task_id="probes",
        cmds=["script/bqetl", "data_governance", "probes", DATE],
        image=DOCKER_IMAGE,
        container_resources=default_resources,
        execution_timeout=timedelta(hours=1),
        secrets=[datahub_gms_token],
    )

    classify = GKEPodOperator(
        task_id="classify",
        cmds=[
            "script/bqetl",
            "data_governance",
            "classify",
            "--run-id={{ run_id }}",
            "--dlp-quota-project=moz-fx-data-shared-prod",
        ],
        arguments="{{ params.targets }}",
        image=DOCKER_IMAGE,
        container_resources=default_resources,
        execution_timeout=timedelta(hours=12),
        # A column the model never answers for fails the run on purpose. The
        # retry is what clears it. In that case already classified columns
        # are skipped so only the failures are redone.
        retries=3,
        retry_delay=timedelta(minutes=15),
        reattach_on_restart=True,
    )

    profile >> lineage >> probes >> classify
