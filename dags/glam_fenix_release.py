from datetime import datetime, timedelta
from functools import partial, reduce

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.glam_subdags.generate_query import (
    generate_and_run_glean_task,
)
from utils.tags import Tag

default_args = {
    "owner": "efilho@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 22),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akomarzewski@mozilla.com",
        "efilho@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

PROJECT = "moz-fx-glam-prod"

tags = [Tag.ImpactTier.tier_1]

with DAG(
    "glam_fenix_release",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 10 * * 6",  # 10am on Saturday
    doc_md=__doc__,
    tags=tags,
) as dag:
    wait_for_glam_fenix = ExternalTaskSensor(
        task_id="wait_for_daily_fenix_release",
        external_dag_id="glam_fenix",
        external_task_id="org_mozilla_fenix_glam_release_done",
        execution_delta=timedelta(days=-5, hours=-16),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    for product in ["org_mozilla_fenix_glam_release"]:
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
            for sample_range in ([0, 19], [20, 39], [40, 59], [60, 79], [80, 99]):
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

        fenix_release_done = EmptyOperator(task_id="fenix_release_done")

        (
            wait_for_glam_fenix
            >> scalar_bucket_counts
            >> scalar_probe_counts
            >> probe_counts
        )
        (
            wait_for_glam_fenix
            >> histogram_bucket_counts
            >> histogram_probe_counts
            >> probe_counts
        )
        probe_counts >> sample_counts >> extract_probe_counts >> fenix_release_done
        (
            wait_for_glam_fenix
            >> user_counts
            >> extract_user_counts
            >> fenix_release_done
        )
        wait_for_glam_fenix >> fenix_release_done
