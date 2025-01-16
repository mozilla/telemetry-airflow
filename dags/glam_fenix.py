"""
Firefox for Android ETL for https://glam.telemetry.mozilla.org/.

Generates and runs a series of BQ queries, see
[bigquery_etl/glam](https://github.com/mozilla/bigquery-etl/tree/main/bigquery_etl/glam)
in bigquery-etl and the
[glam_subdags](https://github.com/mozilla/telemetry-airflow/tree/main/dags/glam_subdags)
in telemetry-airflow.
"""

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
    "org_mozilla_fenix",  # 2019-06-29 - 2020-07-03 (beta), 2020-07-03 - present (nightly)
    "org_mozilla_fenix_nightly",  # 2019-06-30 - 2020-07-03
    "org_mozilla_firefox",  # 2020-07-28 - present
    "org_mozilla_firefox_beta",  # 2020-03-26 - present
    "org_mozilla_fennec_aurora",  # 2020-01-21 - 2020-07-03
]

# This is only required if there is a logical mapping defined within the
# bigquery-etl module within the templates/logical_app_id folder. This builds
# the dependency graph such that the view with the logical clients daily table
# is always pointing to a concrete partition in BigQuery.
LOGICAL_MAPPING = {
    "org_mozilla_fenix_glam_nightly": [
        "org_mozilla_fenix_nightly",
        "org_mozilla_fenix",
        "org_mozilla_fennec_aurora",
    ],
    "org_mozilla_fenix_glam_beta": ["org_mozilla_fenix", "org_mozilla_firefox_beta"],
    "org_mozilla_fenix_glam_release": ["org_mozilla_firefox"],
}

tags = [Tag.ImpactTier.tier_1]

with DAG(
    "glam_fenix",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 2 * * *",
    doc_md=__doc__,
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

        clients_scalar_aggregates_init = init(
            task_name=f"{product}__clients_scalar_aggregates_v1"
        )
        clients_scalar_aggregates = query(
            task_name=f"{product}__clients_scalar_aggregates_v1"
        )

        clients_histogram_aggregates_init = init(
            task_name=f"{product}__clients_histogram_aggregates_v1"
        )
        clients_histogram_aggregates = query(
            task_name=f"{product}__clients_histogram_aggregates_v1"
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

        done = EmptyOperator(task_id=f"{product}_done")

        # set all of the dependencies for all of the tasks
        # get the dependencies for the logical mapping, or just pass through the
        # daily query unmodified
        for dependency in LOGICAL_MAPPING.get(product, [product]):
            mapping[dependency] >> clients_daily_scalar_aggregates
            mapping[dependency] >> clients_daily_histogram_aggregates

        # only the scalar aggregates are upstream of latest versions
        clients_daily_scalar_aggregates >> latest_versions
        latest_versions >> clients_scalar_aggregates_init
        latest_versions >> clients_histogram_aggregates_init

        (
            clients_daily_scalar_aggregates
            >> clients_scalar_aggregates_init
            >> clients_scalar_aggregates
        )
        (
            clients_daily_histogram_aggregates
            >> clients_histogram_aggregates_init
            >> clients_histogram_aggregates
        )

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
