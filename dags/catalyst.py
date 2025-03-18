"""
DAG to schedule generation of performance reports for recently completed nimbus experiments.

See the [catalyst repository](https://github.com/mozilla/catalyst).

*Triage notes*

This app will perform some bigquery queries, and generate statistical reports based on that data which are
then published to https://protosaur.dev/perf-reports/index.html.

Generally, there should be minimal triage necessary for failures unless it's related to infrastructure issues.
Any failures related to the app execution itself will be taken care of directly by the performance team.

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "dpalmeiro@mozilla.com",
    "email": [
        "dpalmeiro@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 5),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "catalyst",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    # Built from repo https://github.com/mozilla/catalyst
    catalyst_image = "gcr.io/moz-fx-data-experiments/catalyst:latest"

    catalyst_run = GKEPodOperator(
        task_id="catalyst_run",
        name="catalyst_run",
        image=catalyst_image,
        email=default_args["email"],
        dag=dag,
    )

    wait_for_clients_daily_export = ExternalTaskSensor(
        task_id="wait_for_clients_daily",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_search_clients_daily = ExternalTaskSensor(
        task_id="wait_for_search_clients_daily",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_bq_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    catalyst_run.set_upstream(
        [
            wait_for_clients_daily_export,
            wait_for_search_clients_daily,
            wait_for_bq_events,
            wait_for_copy_deduplicate_events,
        ]
    )
