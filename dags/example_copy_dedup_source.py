"""Example: simplified copy_deduplicate that only knows about the task-markers DAG.

This demonstrates the "intermediate task-markers DAG" pattern:

    example_copy_dedup_source ─▶ example_copy_dedup_task_markers ─▶ example_downstream_bqetl_*

Clearing `copy_deduplicate_all` here clears the matching sensor in the
task-markers DAG, which clears its marker children, which in turn clear every
downstream bqetl sensor. Adding a new downstream bqetl DAG requires no change
to this file — only the (generated) task-markers DAG needs to learn about it.
"""

import datetime

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker

TASK_MARKERS_DAG_ID = "example_copy_dedup_task_markers"

default_args = {
    "owner": "example@mozilla.com",
    "start_date": datetime.datetime(2026, 4, 1),
    "retries": 0,
}

with models.DAG(
    "example_copy_dedup_source",
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:
    copy_deduplicate_all = EmptyOperator(task_id="copy_deduplicate_all")
    copy_deduplicate_main_ping = EmptyOperator(task_id="copy_deduplicate_main_ping")

    # One marker per "source" task, pointing at the single task-markers DAG.
    # The task-markers DAG owns the list of bqetl consumers; this file is stable.
    task_markers_for_all = ExternalTaskMarker(
        task_id="task_markers__copy_deduplicate_all",
        external_dag_id=TASK_MARKERS_DAG_ID,
        external_task_id="wait_for_copy_deduplicate_all",
        # task-markers DAG runs on the same daily schedule as this DAG
        execution_date="{{ execution_date.isoformat() }}",
    )

    task_markers_for_main_ping = ExternalTaskMarker(
        task_id="task_markers__copy_deduplicate_main_ping",
        external_dag_id=TASK_MARKERS_DAG_ID,
        external_task_id="wait_for_copy_deduplicate_main_ping",
        execution_date="{{ execution_date.isoformat() }}",
    )

    copy_deduplicate_all >> task_markers_for_all
    copy_deduplicate_main_ping >> task_markers_for_main_ping
