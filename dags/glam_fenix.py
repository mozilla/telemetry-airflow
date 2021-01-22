from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from glam_subdags.generate_query import generate_and_run_glean_query
from utils.gcp import gke_command

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 19),
    "email": [
        "telemetry-alerts@mozilla.com",
        "amiyaguchi@mozilla.com",
        "bewu@mozilla.com",
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

dag = DAG("glam_fenix", default_args=default_args, schedule_interval="0 2 * * *")

wait_for_copy_deduplicate = ExternalTaskSensor(
    task_id="wait_for_copy_deduplicate",
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_all",
    execution_delta=timedelta(hours=1),
    check_existence=True,
    mode="reschedule",
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    dag=dag,
)

mapping = {}
for product in PRODUCTS:
    query = generate_and_run_glean_query(
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
    query = generate_and_run_glean_query(
        task_id=f"incremental_{product}",
        product=product,
        destination_project_id=PROJECT,
        env_vars=dict(STAGE="incremental"),
        dag=dag,
    )
    # get the dependencies for the logical mapping, or just pass through the
    # daily query unmodified
    for dependency in LOGICAL_MAPPING.get(product, [product]):
        mapping[dependency] >> query

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
        docker_image="mozilla/bigquery-etl:latest",
        gcp_conn_id="google_cloud_derived_datasets",
        dag=dag,
    )

    query >> export
