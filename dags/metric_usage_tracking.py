from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

from operators.gcp_container_operator import GKEPodOperator, OnFinishAction
from utils.tags import Tag

DOCS = """\
# Metric usage tracking

Finds where each Glean metric and legacy telemetry probe is used (bigquery-etl,
Redash, Looker, GLAM, metric-hub and ad hoc BigQuery jobs) and writes the tables
behind the metric usage page to `moz-fx-data-shared-prod.metric_usage_tracking_derived`.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/metric-usage).

*Triage notes*

Each source is its own task. A step that fails exits without writing, so its
tables keep the last complete run and the task can be cleared and rerun alone.
`summary` runs only after every source succeeds. As long as the most recent
run succeeded, past failures can be ignored.

`adhoc` reads `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, which keeps 180 days. If it
fails for longer than that, the days in between are lost from `adhoc_jobs`.

#### Owner

bewu@mozilla.com
"""

default_args = {
    "owner": "bewu@mozilla.com",
    "email": [
        "benwu@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 25),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3, Tag.Repo.airflow]

IMAGE = "us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/metric-usage:latest"

# Every pod starts empty, so a step that reads the generated-sql or metric-hub
# clone fetches it first. redash, looker, adhoc and metric_hub read bqetl's UDFs
# from generated-sql, and find none (rather than failing) if it is missing.
FETCH_GENERATED_SQL = "script/fetch_generated_sql.sh"
FETCH_METRIC_HUB = "script/fetch_metric_hub.sh"


def resources(cpu, memory):
    return k8s.V1ResourceRequirements(
        requests={"cpu": cpu, "memory": memory},
        limits={"cpu": cpu, "memory": memory},
    )


looker_client_id = Secret(
    deploy_type="env",
    deploy_target="LOOKERSDK_CLIENT_ID",
    secret="airflow-gke-restricted-secrets",
    key="probe_scraper_secret__looker_api_client_id_prod",
)
looker_client_secret = Secret(
    deploy_type="env",
    deploy_target="LOOKERSDK_CLIENT_SECRET",
    secret="airflow-gke-restricted-secrets",
    key="probe_scraper_secret__looker_api_client_secret_prod",
)


def step(name, fetch=(), **kwargs):
    command = " && ".join([*fetch, f"python -m metric_usage.{name}"])
    return GKEPodOperator(
        task_id=name,
        name=f"metric-usage-{name.replace('_', '-')}",
        image=IMAGE,
        cmds=["sh", "-c"],
        arguments=[command],
        **kwargs,
    )


with DAG(
    "metric_usage_tracking",
    doc_md=DOCS,
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=tags,
) as dag:
    inventory = step("inventory")

    lineage = step(
        "lineage",
        fetch=[FETCH_GENERATED_SQL],
        container_resources=resources("1", "2.5Gi"),
    )

    metric_hub = step(
        "metric_hub",
        fetch=[FETCH_GENERATED_SQL, FETCH_METRIC_HUB],
    )

    redash = step(
        "redash",
        fetch=[FETCH_GENERATED_SQL],
        container_resources=resources("3", "3Gi"),
    )

    looker = step(
        "looker",
        fetch=[FETCH_GENERATED_SQL, FETCH_METRIC_HUB],
        # Mostly waiting on the Looker API but stores artifacts in memory
        container_resources=resources("1", "12Gi"),
        secrets=[looker_client_id, looker_client_secret],
        # Writing to disk reduces memory usage
        env_vars={"METRIC_USAGE_CACHE": "1"},
    )

    glam = step("glam")

    adhoc = step(
        "adhoc",
        fetch=[FETCH_GENERATED_SQL, FETCH_METRIC_HUB],
        container_resources=resources("3", "6Gi"),
    )

    summary = step("summary")

    inventory >> lineage >> metric_hub >> [redash, looker, glam, adhoc] >> summary
