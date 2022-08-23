from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from glam_subdags.generate_query import (
    generate_and_run_glean_queries,
    generate_and_run_glean_task,
)
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import gke_command
from functools import partial
from utils.tags import Tag

default_args = {
    "owner": "akommasani@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 19),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akommasani@mozilla.com",
        "akomarzewski@mozilla.com",
        "efilho@mozilla.com",
        "linhnguyen@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

PROJECT = "moz-fx-data-glam-prod-fca7"
BUCKET = "moz-fx-data-glam-prod-fca7-etl-data"

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

    pre_import = DummyOperator(
        task_id=f'pre_import',
    )

    with TaskGroup('glam_fog_external') as glam_fog_external:
        ExternalTaskMarker(
            task_id="glam_glean_imports__wait_for_fog",
            external_dag_id="glam_glean_imports",
            external_task_id="wait_for_fog",
            execution_date="{{ execution_date.replace(hour=5, minute=0).isoformat() }}",
        )

        pre_import >> glam_fog_external

    mapping = {}
    for product in PRODUCTS:
        query = generate_and_run_glean_queries(
            task_id=f"daily_{product}",
            product=product,
            destination_project_id=PROJECT,
            env_vars=dict(STAGE="daily"),

        )
        mapping[product] = query
        wait_for_copy_deduplicate >> query

    # the set of logical ids and the set of ids that are not mapped to logical ids
    final_products = set(LOGICAL_MAPPING.keys()) | set(PRODUCTS) - set(
        sum(LOGICAL_MAPPING.values(), [])
    )
    for product in final_products:
        func = partial(
            generate_and_run_glean_task,
            product=product,
            destination_project_id=PROJECT,
            env_vars=dict(STAGE="incremental"),

        )
        view, init, query = [
            partial(func, task_type=task_type) for task_type in ["view", "init", "query"]
        ]

        # stage 1 - incremental
        clients_daily_histogram_aggregates = view(
            task_name=f"{product}__view_clients_daily_histogram_aggregates_v1"
        )
        clients_daily_scalar_aggregates = view(
            task_name=f"{product}__view_clients_daily_scalar_aggregates_v1"
        )
        latest_versions = query(task_name=f"{product}__latest_versions_v1")

        clients_scalar_aggregate_init = init(
            task_name=f"{product}__clients_scalar_aggregates_v1"
        )
        clients_scalar_aggregate = query(
            task_name=f"{product}__clients_scalar_aggregates_v1"
        )

        clients_histogram_aggregate_init = init(
            task_name=f"{product}__clients_histogram_aggregates_v1"
        )
        clients_histogram_aggregate = query(
            task_name=f"{product}__clients_histogram_aggregates_v1"
        )

        # stage 2 - downstream for export
        scalar_bucket_counts = query(task_name=f"{product}__scalar_bucket_counts_v1")
        scalar_probe_counts = query(task_name=f"{product}__scalar_probe_counts_v1")
        scalar_percentile = query(task_name=f"{product}__scalar_percentiles_v1")

        histogram_bucket_counts = query(task_name=f"{product}__histogram_bucket_counts_v1")
        histogram_probe_counts = query(task_name=f"{product}__histogram_probe_counts_v1")
        histogram_percentiles = query(task_name=f"{product}__histogram_percentiles_v1")

        probe_counts = view(task_name=f"{product}__view_probe_counts_v1")
        extract_probe_counts = query(task_name=f"{product}__extract_probe_counts_v1")

        user_counts = view(task_name=f"{product}__view_user_counts_v1")
        extract_user_counts = query(task_name=f"{product}__extract_user_counts_v1")

        sample_counts = view(task_name=f"{product}__view_sample_counts_v1")

        export = gke_command(
            task_id=f"export_{product}",
            cmds=["bash"],
            env_vars={
                "SRC_PROJECT": PROJECT,
                "DATASET": "glam_etl",
                "PRODUCT": product,
                "BUCKET": BUCKET,
            },
            command=["script/glam/export_csv"],
            docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
            gcp_conn_id="google_cloud_airflow_gke",

        )

        # set all of the dependencies for all of the tasks

        # get the dependencies for the logical mapping, or just pass through the
        # daily query unmodified
        for dependency in LOGICAL_MAPPING.get(product, [product]):
            mapping[dependency] >> clients_daily_scalar_aggregates
            mapping[dependency] >> clients_daily_histogram_aggregates

        # only the scalar aggregates are upstream of latest versions
        clients_daily_scalar_aggregates >> latest_versions
        latest_versions >> clients_scalar_aggregate_init
        latest_versions >> clients_histogram_aggregate_init

        (
            clients_daily_scalar_aggregates
            >> clients_scalar_aggregate_init
            >> clients_scalar_aggregate
        )
        (
            clients_daily_histogram_aggregates
            >> clients_histogram_aggregate_init
            >> clients_histogram_aggregate
        )

        (
            clients_scalar_aggregate
            >> scalar_bucket_counts
            >> scalar_probe_counts
            >> scalar_percentile
            >> probe_counts
        )
        (
            clients_histogram_aggregate
            >> histogram_bucket_counts
            >> histogram_probe_counts
            >> histogram_percentiles
            >> probe_counts
        )
        probe_counts >> sample_counts >> extract_probe_counts >> export >> pre_import
        clients_scalar_aggregate >> user_counts >> extract_user_counts >> export >> pre_import
        clients_histogram_aggregate >> export >> pre_import
