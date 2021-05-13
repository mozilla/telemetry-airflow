import datetime

from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from utils.gcp import (
    bigquery_etl_copy_deduplicate,
    bigquery_etl_query,
    gke_command,
    bigquery_xcom_query,
)

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

DOCS = """\
# Copy-Deduplicate

This DAG is the root of most derived tables. For each live ping table that the
data pipeline populates, we run a "copy_deduplicate" query once per day to
populate the corresponding stable table.

A few immediate downstream tables are also included in this DAG.

## Workflows

In early 2021, manual reruns of `copy_deduplicate` were leading to empty
partitions, but the root cause has been fixed. See
[bug 1690363](https://bugzilla.mozilla.org/show_bug.cgi?id=1690363).

## Changelog

In April 2021, `copy_deduplicate_main_ping` was moved from a 100-slice
configuration to a single-query configuration, which will change the
performance profile and is intended to be more efficient and slightly
faster. We also increased the number of parallel queries in
`copy_deduplicate_all` to help it finish more quickly and split out
`copy_deduplicate_event_ping` to its own task.
See [telemetry-airflow#1279](
https://github.com/mozilla/telemetry-airflow/pull/1279/files)
"""

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag_name = "copy_deduplicate"

with models.DAG(
    dag_name, schedule_interval="0 1 * * *", doc_md=DOCS, default_args=default_args
) as dag:

    # This single task is responsible for sequentially running copy queries
    # over all the tables in _live datasets into _stable datasets except those
    # that are specifically used in another DAG.
    resources = {
        "request_memory": "10240Mi",
        "request_cpu": None,
        "limit_memory": "20480Mi",
        "limit_cpu": None,
        "limit_gpu": None,
    }

    copy_deduplicate_all = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_all",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        priority_weight=100,
        parallelism=10,
        # Any table listed here under except_tables _must_ have a corresponding
        # copy_deduplicate job elsewhere.
        except_tables=["telemetry_live.main_v4", "telemetry_live.event_v4"],
        node_selectors={"nodepool": "highmem"},
        resources=resources,
    )

    copy_deduplicate_main_ping = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_main_ping",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        only_tables=["telemetry_live.main_v4"],
        priority_weight=100,
        parallelism=5,
        slices=20,
        owner="jklukas@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "relud@mozilla.com",
            "jklukas@mozilla.com",
        ],
    )

    copy_deduplicate_event_ping = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_event_ping",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        only_tables=["telemetry_live.event_v4"],
        priority_weight=100,
        parallelism=1,
        owner="jklukas@mozilla.com",
    )

    # Events.

    event_events = bigquery_etl_query(
        task_id="event_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="event_events_v1",
        dataset_id="telemetry_derived",
        priority_weight=90,
        owner="jklukas@mozilla.com",
        arguments=("--schema_update_option=ALLOW_FIELD_ADDITION",),
    )

    copy_deduplicate_event_ping >> event_events

    bq_main_events = bigquery_etl_query(
        task_id="bq_main_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="main_events_v1",
        dataset_id="telemetry_derived",
        priority_weight=90,
        owner="jklukas@mozilla.com",
        dag=dag,
        arguments=("--schema_update_option=ALLOW_FIELD_ADDITION",),
    )

    copy_deduplicate_main_ping >> bq_main_events

    # Daily and last seen views on top of every Glean application.

    # The core clients first seen dataset is a dependency to glean usage
    # queries. Ideally, it would belong inside of a generated bigquery-etl DAG
    # (e.g. bqetl_core), but this would require splitting this DAG into three
    # separate parts threaded by sensors. Since the first_seen_table will end up
    # being part of the clients daily table in this DAG, it will be easier to
    # reason about dependencies in this single DAG while it is being developed.
    telemetry_derived__core_clients_first_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__core_clients_first_seen__v1",
        destination_table="core_clients_first_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="amiyaguchi@mozilla.com",
        email=["amiyaguchi@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    copy_deduplicate_all >> telemetry_derived__core_clients_first_seen__v1

    gcp_conn_id = "google_cloud_derived_datasets"
    baseline_etl_kwargs = dict(
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        namespace="default",
        image="mozilla/bigquery-etl:latest",
    )
    baseline_args = [
        "--project-id=moz-fx-data-shared-prod",
        "--only=*_stable.baseline_v1",
        "--start_date={{ ds }}",
        "--end_date={{ ds }}"
    ]
    baseline_clients_first_seen = GKEPodOperator(
        task_id="baseline_clients_first_seen",
        name="baseline-clients-first-seen",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.baseline_clients_first_seen_v1",
        ] + baseline_args,
        **baseline_etl_kwargs
    )
    baseline_clients_daily = GKEPodOperator(
        task_id="baseline_clients_daily",
        name="baseline-clients-daily",
         command=[
            "bqetl",
            "query",
            "backfill",
            "*.baseline_clients_daily_v1",
        ] + baseline_args,
        **baseline_etl_kwargs
    )
    baseline_clients_last_seen = GKEPodOperator(
        task_id="baseline_clients_last_seen",
        name="baseline-clients-last-seen",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.baseline_clients_last_seen_v1",
        ] + baseline_args,
        depends_on_past=True,
        **baseline_etl_kwargs
    )

    telemetry_derived__core_clients_first_seen__v1 >> baseline_clients_first_seen
    baseline_clients_first_seen >> baseline_clients_daily
    copy_deduplicate_all >> baseline_clients_daily >> baseline_clients_last_seen
