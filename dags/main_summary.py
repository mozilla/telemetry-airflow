from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.executors import get_default_executor
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.email_schema_change_operator import EmailSchemaChangeOperator
from utils.dataproc import (
    moz_dataproc_pyspark_runner,
    moz_dataproc_jar_runner,
    get_dataproc_parameters,
)
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar
from utils.gcp import (
    bigquery_etl_query,
    bigquery_etl_copy_deduplicate,
    export_to_parquet,
    load_to_bigquery,
    gke_command,
)


default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 27),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

# Make sure all the data for the given day has arrived before running.
# Running at 1am should suffice.
dag = DAG('main_summary', default_args=default_args, schedule_interval='0 1 * * *', max_active_runs=10)

# We copy yesterday's main pings from telemetry_live to telemetry_stable
# at the root of this DAG because telemetry_stable.main_v4 will become
# the source for main_summary, etc. once we are comfortable retiring parquet
# data imports.
copy_deduplicate_main_ping = bigquery_etl_copy_deduplicate(
    task_id="copy_deduplicate_main_ping",
    target_project_id="moz-fx-data-shared-prod",
    only_tables=["telemetry_live.main_v4"],
    parallelism=24,
    slices=100,
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

bq_main_events = bigquery_etl_query(
    task_id="bq_main_events",
    project_id="moz-fx-data-shared-prod",
    destination_table="main_events_v1",
    dataset_id="telemetry_derived",
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    dag=dag,
    arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
)

main_summary = bigquery_etl_query(
    task_id="main_summary",
    destination_table="main_summary_v4",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    sql_file_path="sql/telemetry_derived/main_summary_v4/",
    multipart=True,
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    start_date=datetime(2019, 10, 25),
    dag=dag)

main_summary_bigint_columns = [
    # bigquery does not have 32-bit int, and int->bigint is not a
    # backward compatible schema change in spark, so these are the
    # bigint columns from when main summary was generated in spark, and
    # the rest are converted to 32-bit int for backward compatibility
    "--bigint-columns",
    "search_counts.count",
    "events.timestamp",
    "sample_id",
    "os_service_pack_major",
    "os_service_pack_minor",
    "windows_build_number",
    "windows_ubr",
    "install_year",
    "profile_creation_date",
    "profile_reset_date",
    "session_length",
    "subsession_length",
    "timestamp",
    "e10s_multi_processes",
    "active_addons_count",
    "client_clock_skew",
    "client_submission_latency",
    "gc_max_pause_ms_main_above_150",
    "gc_max_pause_ms_main_above_250",
    "gc_max_pause_ms_main_above_2500",
    "gc_max_pause_ms_content_above_150",
    "gc_max_pause_ms_content_above_250",
    "gc_max_pause_ms_content_above_2500",
    "cycle_collector_max_pause_main_above_150",
    "cycle_collector_max_pause_main_above_250",
    "cycle_collector_max_pause_main_above_2500",
    "cycle_collector_max_pause_content_above_150",
    "cycle_collector_max_pause_content_above_250",
    "cycle_collector_max_pause_content_above_2500",
    "input_event_response_coalesced_ms_main_above_150",
    "input_event_response_coalesced_ms_main_above_250",
    "input_event_response_coalesced_ms_main_above_2500",
    "input_event_response_coalesced_ms_content_above_150",
    "input_event_response_coalesced_ms_content_above_250",
    "input_event_response_coalesced_ms_content_above_2500",
    "ghost_windows_main_above_1",
    "ghost_windows_content_above_1",
]

main_summary_export = SubDagOperator(
    subdag=export_to_parquet(
        table="moz-fx-data-shared-prod:telemetry_derived.main_summary_v4${{ds_nodash}}",
        static_partitions=["submission_date_s3={{ds_nodash}}"],
        arguments=[
            "--partition-by=sample_id",
            "--replace='{{ds_nodash}}' AS submission_date",
            "--maps-from-entries",
        ] + main_summary_bigint_columns,
        parent_dag_name=dag.dag_id,
        dag_name="main_summary_export",
        default_args=default_args,
        num_workers=40),
    task_id="main_summary_export",
    executor=get_default_executor(),
    dag=dag)

addons = bigquery_etl_query(
    task_id="addons",
    destination_table="addons_v2",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    dag=dag)

addon_aggregates = bigquery_etl_query(
    task_id="addon_aggregates",
    destination_table="addon_aggregates_v2",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    dag=dag)

addon_names = bigquery_etl_query(
    task_id="addon_names",
    destination_table="addon_names_v1",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    # This is an unpartitioned table that we recreate each day based on the
    # previous day's addon data in main pings, thus the odd combination of
    # parameters below.
    date_partition_parameter=None,
    parameters=["submission_date:DATE:{{ds}}"],
    dag=dag)

clients_daily = bigquery_etl_query(
    task_id="clients_daily",
    destination_table="clients_daily_v6",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    start_date=datetime(2019, 11, 5),
    dag=dag)

clients_daily_export = SubDagOperator(
    subdag=export_to_parquet(
        table="moz-fx-data-shared-prod:telemetry_derived.clients_daily_v6${{ds_nodash}}",
        static_partitions=["submission_date_s3={{ds_nodash}}"],
        arguments=[
            # restore legacy schema
            "--maps-from-entries",
            "--partition-by",
            "submission_date_s3",
            "--drop",
            "submission_date",
            "total_hours_sum",
            "active_experiment_branch",
            "active_experiment_id",
            "histogram_parent_devtools_canvasdebugger_opened_count_sum",
            "histogram_parent_devtools_developertoolbar_opened_count_sum",
            "histogram_parent_devtools_shadereditor_opened_count_sum",
            "histogram_parent_devtools_webaudioeditor_opened_count_sum",
            "scalar_combined_webrtc_nicer_turn_438s_sum",
            "scalar_parent_aushelper_websense_reg_version",
            "scalar_parent_dom_contentprocess_troubled_due_to_memory_sum",
            "--replace",
            "STRING(sample_id) AS sample_id",
            "CAST(subsession_hours_sum AS DECIMAL(37,6)) AS subsession_hours_sum",
            "TRANSFORM(active_addons, _ -> STRUCT(_.addon_id AS addon_id, _.blocklisted AS blocklisted, _.name AS name, _.user_disabled AS user_disabled, _.app_disabled AS app_disabled, _.version AS version, INT(_.scope) AS scope, _.type AS type, _.foreign_install AS foreign_install, _.has_binary_components AS has_binary_components, INT(_.install_day) AS install_day, INT(_.update_day) AS update_day, INT(_.signed_state) AS signed_state, _.is_system AS is_system, _.is_web_extension AS is_web_extension, _.multiprocess_compatible AS multiprocess_compatible)) AS active_addons",
            "TRANSFORM(scalar_parent_devtools_accessibility_select_accessible_for_node_sum, _ -> STRUCT(_.key AS key, INT(_.value) AS value)) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum",
            "INT(cpu_cores) AS cpu_cores",
            "INT(cpu_count) AS cpu_count",
            "INT(cpu_family) AS cpu_family",
            "INT(cpu_l2_cache_kb) AS cpu_l2_cache_kb",
            "INT(cpu_l3_cache_kb) AS cpu_l3_cache_kb",
            "INT(cpu_model) AS cpu_model",
            "INT(cpu_speed_mhz) AS cpu_speed_mhz",
            "INT(cpu_stepping) AS cpu_stepping",
            "INT(memory_mb) AS memory_mb",
            "INT(profile_age_in_days) AS profile_age_in_days",
            "INT(sandbox_effective_content_process_level) AS sandbox_effective_content_process_level",
            "INT(scalar_parent_browser_engagement_max_concurrent_tab_count_max) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max",
            "INT(scalar_parent_browser_engagement_max_concurrent_window_count_max) AS scalar_parent_browser_engagement_max_concurrent_window_count_max",
            "INT(scalar_parent_browser_engagement_unique_domains_count_max) AS scalar_parent_browser_engagement_unique_domains_count_max",
            "INT(timezone_offset) AS timezone_offset",
        ],
        parent_dag_name=dag.dag_id,
        dag_name="clients_daily_export",
        default_args=default_args,
        num_preemptible_workers=10),
    task_id="clients_daily_export",
    executor=get_default_executor(),
    dag=dag)

clients_first_seen = bigquery_etl_query(
    task_id="clients_first_seen",
    destination_table="clients_first_seen_v1",
    project_id="moz-fx-data-shared-prod",
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    depends_on_past=True,
    start_date=datetime(2020, 5, 5),
    dataset_id="telemetry_derived",
    # This query updates the entire existing table every day rather than appending
    # a new partition, so we need to disable date_partition_parameter and instead
    # pass submission_date as a generic param.
    date_partition_parameter=None,
    parameters=["submission_date:DATE:{{ds}}"],
    dag=dag)

clients_last_seen = bigquery_etl_query(
    task_id="clients_last_seen",
    destination_table="clients_last_seen_v1",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "jklukas@mozilla.com"],
    depends_on_past=True,
    start_date=datetime(2019, 4, 15),
    dag=dag)

exact_mau_by_dimensions = bigquery_etl_query(
    task_id="exact_mau_by_dimensions",
    destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    dag=dag)

exact_mau_by_client_count_dimensions = bigquery_etl_query(
    task_id="exact_mau_by_client_count_dimensions",
    project_id='moz-fx-data-shared-prod',
    destination_table="firefox_desktop_exact_mau28_by_client_count_dimensions_v1",
    dataset_id="telemetry_derived",
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

devtools_panel_usage = bigquery_etl_query(
    task_id="devtools_panel_usage",
    destination_table="devtools_panel_usage_v1",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    start_date=datetime(2019, 11, 25),
    dag=dag)

experiments_daily_active_clients = bigquery_etl_query(
    task_id="experiments_daily_active_clients",
    destination_table="experiments_daily_active_clients_v1",
    dataset_id="telemetry_derived",
    project_id="moz-fx-data-shared-prod",
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    dag=dag)


main_summary.set_upstream(copy_deduplicate_main_ping)
main_summary_export.set_upstream(main_summary)
clients_daily.set_upstream(main_summary)
clients_daily_export.set_upstream(clients_daily)

addons.set_upstream(copy_deduplicate_main_ping)
addon_aggregates.set_upstream(copy_deduplicate_main_ping)
addon_names.set_upstream(copy_deduplicate_main_ping)

clients_first_seen.set_upstream(clients_daily)
clients_last_seen.set_upstream(clients_daily)
exact_mau_by_dimensions.set_upstream(clients_last_seen)
exact_mau_by_client_count_dimensions.set_upstream(clients_last_seen)
devtools_panel_usage.set_upstream(clients_daily)

bq_main_events.set_upstream(copy_deduplicate_main_ping)

experiments_daily_active_clients.set_upstream(clients_daily)
