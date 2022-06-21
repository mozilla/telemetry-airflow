import datetime

from airflow import models
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.utils.task_group import TaskGroup
from utils.gcp import (
    bigquery_etl_copy_deduplicate,
    bigquery_etl_query,
    gke_command,
    bigquery_xcom_query,
)

from utils.gcp import gke_command
from utils.tags import Tag

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
    # If a task fails, retry it twice after waiting at least 5 minutes
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag_name = "copy_deduplicate"
tags = [Tag.ImpactTier.tier_1]

with models.DAG(
    dag_name, schedule_interval="0 1 * * *", doc_md=DOCS, default_args=default_args, tags=tags,
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
        except_tables=[
            "telemetry_live.main_v4",
            "telemetry_live.event_v4",
            "telemetry_live.first_shutdown_v4",
            "telemetry_live.saved_session_v4"],
        node_selectors={"nodepool": "highmem"},
        resources=resources,
    )

    with TaskGroup('copy_deduplicate_all_external') as copy_deduplicate_all_external:
        # list of downstream dependencies consisting of external DAG name and execution delta
        downstream_dependencies = {
            ("bhr_collection", "hour=5, minute=0"),
            ("glam_fenix", "hour=2, minute=0"),
            ("glam_fog", "hour=2, minute=0"),
            ("bqetl_activity_stream", "hour=2, minute=0"),
            ("bqetl_amo_stats", "hour=3, minute=0"),
            ("bqetl_core", "hour=2, minute=0"),
            ("bqetl_ctxsvc_derived", "hour=3, minute=0"),
            ("bqetl_desktop_funnel", "hour=4, minute=0"),
            ("bqetl_event_rollup", "hour=3, minute=0"),
            ("bqetl_experiments_daily", "hour=3, minute=0"),
            ("bqetl_feature_usage", "hour=5, minute=0"),
            ("bqetl_fenix_event_rollup", "hour=2, minute=0"),
            ("bqetl_internal_tooling", "hour=4, minute=0"),
            ("bqetl_internet_outages", "hour=7, minute=0"),
            ("bqetl_messaging_system", "hour=2, minute=0"),
            ("bqetl_mobile_search", "hour=2, minute=0"),
            ("bqetl_monitoring", "hour=2, minute=0"),
            ("bqetl_org_mozilla_fenix_derived", "hour=2, minute=0"),
            ("bqetl_regrets_reporter_summary", "hour=4, minute=0"),
            ("bqetl_search_terms_daily", "hour=3, minute=0")
        }

        for downstream_dependency in downstream_dependencies:
            ExternalTaskMarker(
                task_id=f"{downstream_dependency[0]}__wait_for_copy_deduplicate_all",
                external_dag_id=downstream_dependency[0],
                external_task_id="wait_for_copy_deduplicate_all",
                execution_date="{{ execution_date.replace(" + downstream_dependency[1] + ").isoformat() }}",
            )

        copy_deduplicate_all >> copy_deduplicate_all_external

    # We split out main ping since it's the highest volume and has a distinct
    # set of downstream dependencies.
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

    with TaskGroup('main_ping_external') as main_ping_external:
        downstream_dependencies = {
            ("graphics_telemetry", "hour=3, minute=0"),
            ("glam", "hour=2, minute=0"),
            ("bqetl_addons", "hour=4, minute=0"),
            ("bqetl_amo_stats", "hour=3, minute=0"),
            ("bqetl_desktop_platform", "hour=3, minute=0"),
            ("bqetl_devtools", "hour=3, minute=0"),
            ("bqetl_experiments_daily", "hour=3, minute=0"),
            ("bqetl_fog_decision_support", "hour=4, minute=0"),
            ("bqetl_internet_outages", "hour=7, minute=0"),
            ("bqetl_main_summary", "hour=2, minute=0"),
            ("bqetl_monitoring", "hour=2, minute=0"),
            ("bqetl_ssl_ratios", "hour=2, minute=0")
        }

        for downstream_dependency in downstream_dependencies:
            ExternalTaskMarker(
                task_id=f"{downstream_dependency[0]}__wait_for_copy_deduplicate_main_ping",
                external_dag_id=downstream_dependency[0],
                external_task_id="wait_for_copy_deduplicate_main_ping",
                execution_date="{{ execution_date.replace(" + downstream_dependency[1] + ").isoformat() }}",
            )

        copy_deduplicate_main_ping >> main_ping_external

    # We also separate out variant pings that share the main ping schema since these
    # ultrawide tables can sometimes have unique performance problems.
    copy_deduplicate_first_shutdown_ping = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_first_shutdown_ping",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        only_tables=["telemetry_live.first_shutdown_v4"],
        priority_weight=50,
        parallelism=1,
        owner="jklukas@mozilla.com",
    )

    copy_deduplicate_saved_session_ping = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_saved_session_ping",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        only_tables=["telemetry_live.saved_session_v4"],
        priority_weight=50,
        parallelism=1,
        owner="jklukas@mozilla.com",
    )

    # Events.

    copy_deduplicate_event_ping = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_event_ping",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        only_tables=["telemetry_live.event_v4"],
        priority_weight=100,
        parallelism=1,
        owner="jklukas@mozilla.com",
    )

    event_events = bigquery_etl_query(
        task_id="event_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="event_events_v1",
        dataset_id="telemetry_derived",
        priority_weight=90,
        owner="jklukas@mozilla.com",
        arguments=("--schema_update_option=ALLOW_FIELD_ADDITION",),
    )

    with TaskGroup('event_events_external') as event_events_external:
        downstream_dependencies = {
            ("jetstream", "hour=4, minute=0"),
            ("bqetl_amo_stats", "hour=3, minute=0"),
            ("bqetl_experiments_daily", "hour=3, minute=0"),
            ("bqetl_feature_usage", "hour=5, minute=0"),
            ("bqetl_main_summary", "hour=2, minute=0")
        }

        for downstream_dependency in downstream_dependencies:
            ExternalTaskMarker(
                task_id=f"{downstream_dependency[0]}__wait_for_event_events",
                external_dag_id=downstream_dependency[0],
                external_task_id="wait_for_event_events",
                execution_date="{{ execution_date.replace(" + downstream_dependency[1] + ").isoformat() }}",
            )

        event_events >> event_events_external

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

    with TaskGroup('bq_main_events_external') as bq_main_events_external:
        downstream_dependencies = {
            ("jetstream", "hour=4, minute=0"),
            ("bqetl_amo_stats", "hour=3, minute=0"),
            ("bqetl_experiments_daily", "hour=3, minute=0"),
            ("bqetl_feature_usage", "hour=5, minute=0"),
            ("bqetl_main_summary", "hour=2, minute=0")
        }

        for downstream_dependency in downstream_dependencies:
            ExternalTaskMarker(
                task_id=f"{downstream_dependency[0]}__wait_for_bq_main_events",
                external_dag_id=downstream_dependency[0],
                external_task_id="wait_for_bq_main_events",
                execution_date="{{ execution_date.replace(" + downstream_dependency[1] + ").isoformat() }}",
            )

        bq_main_events >> bq_main_events_external

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

    with TaskGroup('core_clients_first_seen_external') as core_clients_first_seen_external:
        ExternalTaskMarker(
            task_id="bqetl_core__wait_for_core_clients_first_seen",
            external_dag_id="bqetl_core",
            external_task_id="wait_for_telemetry_derived__core_clients_first_seen__v1",
            execution_date="{{ execution_date.replace(hour=2, minute=0).isoformat() }}",
        )

        telemetry_derived__core_clients_first_seen__v1 >> core_clients_first_seen_external

    copy_deduplicate_all >> telemetry_derived__core_clients_first_seen__v1

    baseline_args = [
        "--project-id=moz-fx-data-shared-prod",
        "--start_date={{ ds }}",
        "--end_date={{ ds }}",
        "--max_rows=0",
    ]

    baseline_clients_first_seen = gke_command(
        task_id="baseline_clients_first_seen",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.baseline_clients_first_seen_v1",
            "--no-partition",
        ] + baseline_args,
        depends_on_past=True,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )
    baseline_clients_daily = gke_command(
        task_id="baseline_clients_daily",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.baseline_clients_daily_v1",
        ] + baseline_args,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )
    baseline_clients_last_seen = gke_command(
        task_id="baseline_clients_last_seen",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.baseline_clients_last_seen_v1",
        ] + baseline_args,
        depends_on_past=True,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )

    with TaskGroup('baseline_clients_last_seen_external') as baseline_clients_last_seen_external:
        downstream_dependencies = {
            ("bqetl_gud", "hour=3, minute=0"),
            ("bqetl_nondesktop", "hour=3, minute=0")
        }

        for downstream_dependency in downstream_dependencies:
            ExternalTaskMarker(
                task_id=f"{downstream_dependency[0]}__wait_for_baseline_clients_last_seen",
                external_dag_id=downstream_dependency[0],
                external_task_id="wait_for_baseline_clients_last_seen",
                execution_date="{{ execution_date.replace(" + downstream_dependency[1] + ").isoformat() }}",
            )

        baseline_clients_last_seen >> baseline_clients_last_seen_external

    metrics_clients_daily = gke_command(
        task_id="metrics_clients_daily",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.metrics_clients_daily_v1",
        ] + baseline_args,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )
    metrics_clients_last_seen = gke_command(
        task_id="metrics_clients_last_seen",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.metrics_clients_last_seen_v1",
        ] + baseline_args,
        depends_on_past=True,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )
    clients_last_seen_joined = gke_command(
        task_id="clients_last_seen_joined",
        command=[
            "bqetl",
            "query",
            "backfill",
            "*.clients_last_seen_joined_v1",
        ] + baseline_args,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )

    telemetry_derived__core_clients_first_seen__v1 >> baseline_clients_first_seen
    baseline_clients_first_seen >> baseline_clients_daily
    copy_deduplicate_all >> baseline_clients_daily >> baseline_clients_last_seen
    copy_deduplicate_all >> metrics_clients_daily >> metrics_clients_last_seen
    metrics_clients_last_seen >> clients_last_seen_joined
    baseline_clients_last_seen >> clients_last_seen_joined
