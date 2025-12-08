"""
Run dryrun validation for bigquery-etl after deployment.

This DAG is triggered by the bqetl_artifact_deployment DAG after views are published.
It runs in parallel and does not block the deployment DAG from completing.

*Triage notes*

Check logs for dryruns that returned an error.
Some dryruns may fail if artifact dependencies haven't been deployed yet.
For failures due to no access or persistent timeouts, add the query to the skip list in `bqetl_project.yaml`:
https://github.com/mozilla/bigquery-etl/blob/3742cad36d606edfd928524ec3a1e76e94efa315/bqetl_project.yaml#L45
"""

from datetime import datetime

from airflow import DAG
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 6),
    "email_on_failure": True,
    "retries": 0,
}

tags = [Tag.ImpactTier.tier_2]


def should_dryrun(dag_id: str, generate_sql: bool) -> bool:
    """
    Run deploys if there are no other queued dag runs or if the generate_sql param is true.

    When used with ShortCircuitOperator, true means run downstream tasks and false means skip.
    """
    queued_runs = DagRun.find(dag_id=dag_id, state=DagRunState.QUEUED)
    print(f"Found {len(queued_runs)} queued dag runs for {dag_id}")
    return len(queued_runs) == 0 or generate_sql == "True"


with DAG(
    "bqetl_dryrun",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    doc_md=__doc__,
    tags=tags,
) as dag:
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

    skip_if_queued_runs_exist = ShortCircuitOperator(
        task_id="skip_if_queued_runs_exist",
        ignore_downstream_trigger_rules=True,
        python_callable=should_dryrun,
        op_kwargs={"dag_id": dag.dag_id},
    )

    dryrun = GKEPodOperator(
        task_id="dryrun",
        arguments=["script/bqetl", "dryrun", "--validate-schemas", "sql"],
        image=docker_image,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dryrun.set_upstream(skip_if_queued_runs_exist)
