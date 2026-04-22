"""Example: generated task-markers DAG that mirrors source tasks to every consumer.

This DAG would be emitted by bigquery-etl's DAG generator, alongside the
bqetl_* DAGs themselves. Since the generator already knows which bqetl DAGs
wait on which copy_deduplicate tasks (it's writing those sensors), it can
emit the symmetric marker graph here without any hand-maintained list.

Shape:

    wait_for_copy_deduplicate_all ─▶ [marker per bqetl consumer]
    wait_for_copy_deduplicate_main_ping ─▶ [marker per bqetl consumer]

When the upstream source task is cleared, the marker in source points here,
the sensor is cleared, each marker in the TaskGroup fires, and every
downstream bqetl DAG's `wait_for_*` sensor is cleared in turn.
"""

import datetime
from datetime import timedelta

from airflow import models
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

SOURCE_DAG_ID = "example_copy_dedup_source"

ALLOWED_STATES = ["success"]
FAILED_STATES = ["failed", "upstream_failed", "skipped"]

# This mapping is what bigquery-etl's generator would emit. It mirrors the
# sensors the generator already creates inside each bqetl DAG:
#   { source_task_id: [(downstream_dag_id, downstream_schedule_hour_minute), ...] }
# Schedule tuples are (hour, minute) for the downstream DAG's logical date.
DOWNSTREAM_BY_SOURCE: dict[str, list[tuple[str, tuple[int, int]]]] = {
    "copy_deduplicate_all": [
        ("example_downstream_bqetl_a", (2, 0)),
        ("example_downstream_bqetl_b", (4, 15)),
    ],
    "copy_deduplicate_main_ping": [
        ("example_downstream_bqetl_a", (2, 0)),
    ],
}

default_args = {
    "owner": "example@mozilla.com",
    "start_date": datetime.datetime(2026, 4, 1),
    "retries": 0,
}

with models.DAG(
    "example_copy_dedup_task_markers",
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["example", "generated"],
) as dag:
    for source_task_id, consumers in DOWNSTREAM_BY_SOURCE.items():
        # Sensor waits for the source task in example_copy_dedup_source.
        # Same schedule → execution_delta is zero.
        sensor = ExternalTaskSensor(
            task_id=f"wait_for_{source_task_id}",
            external_dag_id=SOURCE_DAG_ID,
            external_task_id=source_task_id,
            execution_delta=timedelta(0),
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
        )

        with TaskGroup(f"{source_task_id}_task_markers") as task_markers_group:
            for downstream_dag_id, (hour, minute) in consumers:
                ExternalTaskMarker(
                    task_id=f"{downstream_dag_id}__wait_for_{source_task_id}",
                    external_dag_id=downstream_dag_id,
                    external_task_id=f"wait_for_{source_task_id}",
                    execution_date=(
                        "{{ execution_date.replace("
                        f"hour={hour}, minute={minute}"
                        ").isoformat() }}"
                    ),
                )

        sensor >> task_markers_group
