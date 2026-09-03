"""
See [highwind in the docker-etl repository](https://github.com/mozilla/docker-etl/tree/main/jobs/highwind).

Highwind is a proof of concept for Nimbus experiment analysis. It aggregates per-branch sufficient
statistics for Firefox Desktop guardrail metrics in BigQuery, then computes CUPED-adjusted
sequential confidence intervals from them. One run is a full recompute of every live Firefox
Desktop experiment: it reads the experiment mirror, aggregates each metric over windows anchored to
each analysis unit's own enrollment, and writes results to the `highwind_poc` dataset in
`moz-fx-data-experiments` plus one JSON blob per experiment.

It runs alongside Jetstream and replaces nothing.
"""

from collections import namedtuple
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "jkerim@mozilla.com",
    "email": ["jkerim@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 3),
    "email_on_failure": True,
    "email_on_retry": False,
    # A run recomputes every live experiment, so a retry costs a second full run's worth of
    # scanning. The job already isolates per-experiment failures and exits non-zero only when every
    # experiment failed, which is not a condition a retry fixes.
    "retries": 0,
}

TAGS = [Tag.ImpactTier.tier_3]
IMAGE = "us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/highwind:latest"

Upstream = namedtuple("Upstream", ["name", "dag_id", "task_id", "execution_delta"])

# The tables the job reads, and how far ahead of this DAG each one's task is scheduled. The sensors
# wait on the specific tasks rather than on a fixed offset from the schedule, so a slow upstream run
# delays this one instead of being read half-written.
UPSTREAM = [
    Upstream(
        "clients_daily",
        "bqetl_main_summary",
        "telemetry_derived__clients_daily__v6",
        timedelta(hours=9),
    ),
    Upstream(
        "clients_last_seen",
        "bqetl_main_summary",
        "telemetry_derived__clients_last_seen__v2",
        timedelta(hours=9),
    ),
    Upstream(
        "search_clients_daily",
        "bqetl_search",
        "search_derived__search_clients_daily__v8",
        timedelta(hours=8),
    ),
]

with DAG(
    "highwind",
    default_args=default_args,
    # After bqetl_main_summary (02:00) and bqetl_search (03:00), far enough back that the sensors
    # below are short in the ordinary case.
    schedule_interval="0 11 * * *",
    doc_md=__doc__,
    tags=TAGS,
    # A run writes the whole partition, so two overlapping runs would each scan everything and race
    # to write the same rows.
    max_active_runs=1,
    catchup=False,
) as dag:
    highwind = GKEPodOperator(
        task_id="highwind",
        # The image's ENTRYPOINT is python, so the arguments start at -m rather than repeating it.
        arguments=["-m", "highwind.main", "--date", "{{ ds }}"],
        image=IMAGE,
        # Headroom rather than a real bound: a full run finishes well inside an hour. It is here so
        # a pathological run cannot hold slots all the way to BigQuery's own six hour query limit.
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )

    for upstream in UPSTREAM:
        wait_for_upstream = ExternalTaskSensor(
            task_id=f"wait_for_{upstream.name}",
            external_dag_id=upstream.dag_id,
            external_task_id=upstream.task_id,
            execution_delta=upstream.execution_delta,
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )

        wait_for_upstream >> highwind
