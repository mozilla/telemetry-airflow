"""
Firefox for Android ETL for https://glam.telemetry.mozilla.org/
Generates and runs a series of BQ queries, see
[bigquery_etl/glam](https://github.com/mozilla/bigquery-etl/tree/main/bigquery_etl/glam)
in bigquery-etl and the
[glam_subdags](https://github.com/mozilla/telemetry-airflow/tree/main/dags/glam_subdags)
in telemetry-airflow.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators.gcp_container_operator import GKENatPodOperator
from operators.task_sensor import ExternalTaskCompletedSensor
from glam_subdags.generate_query import (
    generate_and_run_glean_queries,
    generate_and_run_glean_task,
)
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

dag = DAG(
    "glam_fenix",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 2 * * *",
    doc_md=__doc__,
    tags=tags,
)

wait_for_copy_deduplicate = ExternalTaskCompletedSensor(
    task_id="wait_for_copy_deduplicate",
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_all",
    execution_delta=timedelta(hours=1),
    check_existence=True,
    mode="reschedule",
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)

pre_import = DummyOperator(
    task_id=f'pre_import',
    dag=dag,
)

mapping = {}
for product in PRODUCTS:
    query = generate_and_run_glean_queries(
        task_id=f"daily_{product}",
        product=product,
        destination_project_id=PROJECT,
        env_vars=dict(STAGE="daily"),
        dag=dag,
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
        dag=dag,
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
    extract_sample_counts = query(task_name=f"{product}__extract_sample_counts_v1")

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
        gcp_conn_id="google_cloud_derived_datasets",
        dag=dag,
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
    probe_counts >> extract_probe_counts >> export >> pre_import
    clients_scalar_aggregate >> user_counts >> extract_user_counts >> export >> pre_import
    clients_histogram_aggregate >> sample_counts >> extract_sample_counts >> export >> pre_import

# Move logic from Glam deployment's GKE Cronjob to this dag for better dependency timing
glam_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2021.8.1-10'

base_docker_args = ['/venv/bin/python', 'manage.py']

env_vars = dict(
    DATABASE_URL = Variable.get("glam_secret__database_url"),
    DJANGO_SECRET_KEY = Variable.get("glam_secret__django_secret_key"),
    DJANGO_CONFIGURATION = "Prod",
    DJANGO_DEBUG = "False",
    DJANGO_SETTINGS_MODULE = "glam.settings",
    GOOGLE_CLOUD_PROJECT = "moz-fx-data-glam-prod-fca7"
)

glam_fenix_import_glean_aggs_beta = GKENatPodOperator(
    task_id = 'glam_fenix_import_glean_aggs_beta',
    name = 'glam_fenix_import_glean_aggs_beta',
    image = glam_import_image,
    arguments = base_docker_args + ['import_glean_aggs', 'beta'],
    env_vars = env_vars,
    dag=dag)

glam_fenix_import_glean_aggs_nightly = GKENatPodOperator(
    task_id = 'glam_fenix_import_glean_aggs_nightly',
    name = 'glam_fenix_import_glean_aggs_nightly',
    image = glam_import_image,
    arguments = base_docker_args + ['import_glean_aggs', 'nightly'],
    env_vars = env_vars,
    dag=dag)

glam_fenix_import_glean_aggs_release = GKENatPodOperator(
    task_id = 'glam_fenix_import_glean_aggs_release',
    name = 'glam_fenix_import_glean_aggs_release',
    image = glam_import_image,
    arguments = base_docker_args + ['import_glean_aggs', 'release'],
    env_vars = env_vars,
    dag=dag)

glam_fenix_import_glean_counts = GKENatPodOperator(
    task_id = 'glam_fenix_import_glean_counts',
    name = 'glam_fenix_import_glean_counts',
    image = glam_import_image,
    arguments = base_docker_args + ['import_glean_counts'],
    env_vars = env_vars,
    dag=dag)


pre_import >> glam_fenix_import_glean_aggs_beta
pre_import >> glam_fenix_import_glean_aggs_nightly
pre_import >> glam_fenix_import_glean_aggs_release
pre_import >> glam_fenix_import_glean_counts