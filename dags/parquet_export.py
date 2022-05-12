"""
Export of a few BigQuery datasets to Parquet files on GCS.

The only consumer of these datasets is the taar DAGs.
We should eventually update the TAAR logic to use BigQuery directly,
which would allow us to tear down this DAG.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import (
    bigquery_etl_query,
    bigquery_etl_copy_deduplicate,
    export_to_parquet,
    gke_command,
)
from utils.tags import Tag


default_args = {
    'owner': 'dthorn@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 27),
    'email': ['telemetry-alerts@mozilla.com', 'dthorn@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

# Make sure all the data for the given day has arrived before running.
# Running at 1am should suffice.
with DAG('parquet_export', default_args=default_args, schedule_interval='0 3 * * *', doc_md=__doc__, tags=tags,) as dag:
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
            table="moz-fx-data-shared-prod.telemetry_derived.main_summary_v4${{ds_nodash}}",
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
        dag=dag)

    clients_daily_export = SubDagOperator(
        subdag=export_to_parquet(
            table="moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6${{ds_nodash}}",
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
        dag=dag)

    with TaskGroup('clients_daily_export_external') as clients_daily_export_external:
        ExternalTaskMarker(
            task_id="taar_daily__wait_for_clients_daily_export",
            external_dag_id="taar_daily",
            external_task_id="wait_for_clients_daily_export",
            execution_date="{{ execution_date.replace(hour=4, minute=0).isoformat() }}",
        )

        clients_daily_export >> clients_daily_export_external

    wait_for_clients_daily = ExternalTaskSensor(
        task_id="wait_for_clients_daily",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag)

    wait_for_main_summary = ExternalTaskSensor(
        task_id="wait_for_main_summary",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_summary__v4",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag)

    main_summary_export.set_upstream(wait_for_main_summary)
    clients_daily_export.set_upstream(wait_for_clients_daily)
