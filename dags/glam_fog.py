import operator
from datetime import datetime, timedelta
from functools import partial, reduce

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.glam_subdags.generate_query import (
    generate_and_run_glean_queries,
    generate_and_run_glean_task,
)
from utils.tags import Tag

default_args = {
    "owner": "efilho@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 19),
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

PROJECT = "moz-fx-glam-prod"

# Fenix as a product has a convoluted app_id history. The comments note the
# start and end dates of the id in the app store.
# https://docs.google.com/spreadsheets/d/18PzkzZxdpFl23__-CIO735NumYDqu7jHpqllo0sBbPA
PRODUCTS = [
    "firefox_desktop",
]

# This is only required if there is a logical mapping defined within the
# bigquery-etl module within the templates/logical_app_id folder. This builds
# the dependency graph such that the view with the logical clients daily table
# is always pointing to a concrete partition in BigQuery.
LOGICAL_MAPPING = {
    "firefox_desktop_glam_nightly": ["firefox_desktop"],
    "firefox_desktop_glam_beta": ["firefox_desktop"],
    "firefox_desktop_glam_release": ["firefox_desktop"],
}

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "glam_fog",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 2 * * *",
    tags=tags,
) as dag:
    wait_for_copy_deduplicate = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    daily_release_done = EmptyOperator(
        task_id="daily_release_done",
    )

    mapping = {}
    for product in PRODUCTS:
        query = generate_and_run_glean_queries(
            task_id=f"daily_{product}",
            product=product,
            destination_project_id=PROJECT,
            env_vars={"STAGE": "daily"},
        )
        mapping[product] = query
        wait_for_copy_deduplicate >> query

    # the set of logical ids and the set of ids that are not mapped to logical ids
    final_products = set(LOGICAL_MAPPING.keys()) | set(PRODUCTS) - set(
        reduce(operator.iadd, LOGICAL_MAPPING.values(), [])
    )
    for product in final_products:
        is_release = "release" in product
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

        # stage 1 - incremental
        clients_daily_histogram_aggregates = view(
            task_name=f"{product}__view_clients_daily_histogram_aggregates_v1"
        )
        clients_daily_scalar_aggregates = view(
            task_name=f"{product}__view_clients_daily_scalar_aggregates_v1"
        )
        latest_versions = query(task_name=f"{product}__latest_versions_v1")

        scalar_wait_for_yesterdays_aggregates = ExternalTaskSensor(
            task_id=f"{product}_wait_for_yesterdays_scalar_aggregates",
            external_dag_id="glam_fog",
            external_task_id=f"query_{product}__clients_scalar_aggregates_v1",
            execution_delta=timedelta(days=1),
            mode="reschedule",
        )
        clients_scalar_aggregates_new_init = init(
            task_name=f"{product}__clients_scalar_aggregates_new_v1"
        )
        clients_scalar_aggregates_new = query(
            task_name=f"{product}__clients_scalar_aggregates_new_v1"
        )
        clients_scalar_aggregates_init = init(
            task_name=f"{product}__clients_scalar_aggregates_v1"
        )
        clients_scalar_aggregates = query(
            task_name=f"{product}__clients_scalar_aggregates_v1"
        )
        clients_histogram_aggregates_new_init = init(
            task_name=f"{product}__clients_histogram_aggregates_new_v1"
        )
        clients_histogram_aggregates_new = query(
            task_name=f"{product}__clients_histogram_aggregates_new_v1"
        )
        clients_histogram_aggregates_init = init(
            task_name=f"{product}__clients_histogram_aggregates_v1"
        )
        histogram_wait_for_yesterdays_aggregates_partial = partial(
            ExternalTaskSensor,
            task_id=f"{product}_wait_for_yesterdays_histogram_aggregates",
            external_dag_id="glam_fog",
            execution_delta=timedelta(days=1),
            mode="reschedule",
        )
        if is_release:
            histogram_wait_for_yesterdays_aggregates = (
                histogram_wait_for_yesterdays_aggregates_partial(
                    external_task_group_id=(
                        f"query_{product}__clients_histogram_aggregates_v1"
                    ),
                )
            )
            clients_histogram_aggregates_snapshot_init = init(
                task_name=f"{product}__clients_histogram_aggregates_snapshot_v1"
            )
            with TaskGroup(
                group_id=f"{product}__clients_histogram_aggregates_v1", dag=dag, default_args=default_args
            ) as clients_histogram_aggregates:
                prev_task = None
                for sample_range in (
                    [0, 2], [3, 5], [6, 9], [10, 49], [50, 99]
                ):
                    clients_histogram_aggregates_sampled = query(
                        task_name=(
                            f"{product}__clients_histogram_aggregates_v1_sampled_"
                            f"{sample_range[0]}_{sample_range[1]}"
                        ),
                        min_sample_id=sample_range[0],
                        max_sample_id=sample_range[1],
                        replace_table=(sample_range[0] == 0)
                    )
                    if prev_task:
                        clients_histogram_aggregates_sampled.set_upstream(prev_task)
                    prev_task = clients_histogram_aggregates_sampled
        else:
            histogram_wait_for_yesterdays_aggregates = (
                histogram_wait_for_yesterdays_aggregates_partial(
                    external_task_id=(
                        f"query_{product}__clients_histogram_aggregates_v1"
                    ),
                )
            )
            clients_histogram_aggregates = query(
                task_name=f"{product}__clients_histogram_aggregates_v1"
            )

        # set all of the dependencies for all of the tasks

        # get the dependencies for the logical mapping, or just pass through the
        # daily query unmodified
        for dependency in LOGICAL_MAPPING.get(product, [product]):
            mapping[dependency] >> clients_daily_scalar_aggregates
            mapping[dependency] >> clients_daily_histogram_aggregates
        clients_daily_scalar_aggregates >> latest_versions
        clients_daily_histogram_aggregates >> latest_versions

        latest_versions >> scalar_wait_for_yesterdays_aggregates
        latest_versions >> histogram_wait_for_yesterdays_aggregates

        (
            scalar_wait_for_yesterdays_aggregates
            >> clients_scalar_aggregates_new_init
            >> clients_scalar_aggregates_new
            >> clients_scalar_aggregates_init
            >> clients_scalar_aggregates
        )
        (
            histogram_wait_for_yesterdays_aggregates
            >> clients_histogram_aggregates_new_init
            >> clients_histogram_aggregates_new
            >> clients_histogram_aggregates_init
        )

        if is_release:
            clients_histogram_aggregates_init >> clients_histogram_aggregates_snapshot_init
            clients_histogram_aggregates_snapshot_init >> clients_histogram_aggregates
            clients_histogram_aggregates >> daily_release_done
            clients_scalar_aggregates >> daily_release_done
        else:
            clients_histogram_aggregates_init >> clients_histogram_aggregates
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

            done = EmptyOperator(task_id=f"{product}_done")

            (
                clients_scalar_aggregates
                >> scalar_bucket_counts
                >> scalar_probe_counts
                >> probe_counts
            )
            (
                clients_histogram_aggregates
                >> histogram_bucket_counts
                >> histogram_probe_counts
                >> probe_counts
            )
            probe_counts >> sample_counts >> extract_probe_counts >> done
            (
                clients_scalar_aggregates
                >> user_counts
                >> extract_user_counts
                >> done
            )
            clients_histogram_aggregates >> done
