import operator
from datetime import datetime, timedelta
from functools import partial, reduce

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.glam_subdags.generate_query import (
    generate_and_run_glean_task,
)
from utils.tags import Tag

default_args = {
    "owner": "efilho@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 11),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akomarzewski@mozilla.com",
        "efilho@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

PROJECT = "moz-fx-data-glam-prod-fca7"

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "glam_fog_release",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 10 * * 6",  # 10am on Saturday
    tags=tags,
) as dag:
    wait_for_glam_fog = ExternalTaskSensor(
        task_id="wait_for_daily_glam_fog_release",
        external_dag_id="glam_fog",
        external_task_id="daily_release_done",
        execution_delta=timedelta(days=-5, hours=-16),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    fog_release_done = EmptyOperator(
        task_id="fog_release_done",
    )

    for product in ["firefox_desktop_glam_release"]:
        func = partial(
            generate_and_run_glean_task,
            product=product,
            destination_project_id=PROJECT,
            env_vars={"STAGE": "incremental"},
        )
        view, init, query = (
            partial(func, task_type=task_type)
            for task_type in ["view", "init", "query"]
        )

        # stage 2 - downstream for export
        scalar_bucket_counts = query(task_name=f"{product}__scalar_bucket_counts_v1")
        scalar_probe_counts = query(task_name=f"{product}__scalar_probe_counts_v1")

        with TaskGroup(
            group_id=f"{product}__histogram_bucket_counts_v1", dag=dag, default_args=default_args
        ) as histogram_bucket_counts:
            prev_task = None
            # Windows + Release data is in [0-9] so we're further splitting that range.
            for sample_range in ([0, 2], [3, 5], [6, 9], [10, 49], [50, 99]):
                histogram_bucket_counts_sampled = query(
                    task_name=f"{product}__histogram_bucket_counts_v1_sampled_{sample_range[0]}_{sample_range[1]}",
                    min_sample_id=sample_range[0],
                    max_sample_id=sample_range[1],
                    replace_table=(sample_range[0] == 0)
                )
                if prev_task:
                    histogram_bucket_counts_sampled.set_upstream(prev_task)
                prev_task = histogram_bucket_counts_sampled

        histogram_probe_counts = query(
            task_name=f"{product}__histogram_probe_counts_v1"
        )

        probe_counts = view(task_name=f"{product}__view_probe_counts_v1")
        extract_probe_counts = query(task_name=f"{product}__extract_probe_counts_v1")

        user_counts = view(task_name=f"{product}__view_user_counts_v1")
        extract_user_counts = query(task_name=f"{product}__extract_user_counts_v1")

        sample_counts = view(task_name=f"{product}__view_sample_counts_v1")

        (
            wait_for_glam_fog
            >> scalar_bucket_counts
            >> scalar_probe_counts
            >> probe_counts
        )
        (
            wait_for_glam_fog
            >> histogram_bucket_counts
            >> histogram_probe_counts
            >> probe_counts
        )
        probe_counts >> sample_counts >> extract_probe_counts >> fog_release_done
        (
            wait_for_glam_fog
            >> user_counts
            >> extract_user_counts
            >> fog_release_done
        )
        wait_for_glam_fog >> fog_release_done
