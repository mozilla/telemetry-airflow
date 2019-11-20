from airflow import DAG
from datetime import datetime, timedelta
from itertools import chain
from operators.emr_spark_operator import EMRSparkOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.executors import GetDefaultExecutor
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.email_schema_change_operator import EmailSchemaChangeOperator
from utils.dataproc import moz_dataproc_pyspark_runner, moz_dataproc_jar_runner
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar
from utils.status import register_status
from utils.gcp import (
    bigquery_etl_query,
    bigquery_etl_copy_deduplicate,
    export_to_parquet,
    load_to_bigquery,
)

taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(taar_aws_conn_id).get_credentials()
taarlite_cluster_name = "dataproc-taarlite-guidguid"
taar_locale_cluster_name = "dataproc-taar-locale"
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"

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
dag = DAG('main_summary', default_args=default_args, schedule_interval='0 1 * * *')

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
    dag=dag)

main_summary = bigquery_etl_query(
    task_id="main_summary",
    destination_table="main_summary_v4",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    sql_file_path="sql/telemetry_derived/main_summary_v4/",
    multipart=True,
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "pmcdermott@mozilla.com", "dzielaski@mozilla.com", "jmundi@mozilla.com"],
    start_date=datetime(2019, 10, 25),
    dag=dag)

main_summary_export = SubDagOperator(
    subdag=export_to_parquet(
        table="moz-fx-data-shared-prod:telemetry_derived.main_summary_v4${{ds_nodash}}",
        static_partitions="submission_date_s3={{ds_nodash}}",
        arguments=[
            "--partition-by=sample_id",
            "--replace='{{ds_nodash}}' AS submission_date",
            "--maps-from-entries",
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
        ],
        parent_dag_name=dag.dag_id,
        dag_name="main_summary_export",
        default_args=default_args,
        num_workers=40),
    task_id="main_summary_export",
    executor=GetDefaultExecutor(),
    dag=dag)

register_status(main_summary, "Main Summary", "A summary view of main pings.")

addons = EMRSparkOperator(
    task_id="addons",
    job_name="Addons View",
    execution_timeout=timedelta(hours=4),
    instance_count=3,
    env=tbv_envvar("com.mozilla.telemetry.views.AddonsView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

addons_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="addons_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="addons",
        dataset_version="v2",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="addons_bigquery_load",
    dag=dag)

main_events = EMRSparkOperator(
    task_id="main_events",
    job_name="Main Events View",
    execution_timeout=timedelta(hours=4),
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    instance_count=1,
    env=tbv_envvar("com.mozilla.telemetry.views.MainEventsView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

main_events_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="main_events_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="events",
        dataset_version="v1",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="main_events_bigquery_load",
    dag=dag)

addon_aggregates = EMRSparkOperator(
    task_id="addon_aggregates",
    job_name="Addon Aggregates View",
    execution_timeout=timedelta(hours=8),
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    instance_count=10,
    env=mozetl_envvar("addon_aggregates", {
        "date": "{{ ds_nodash }}",
        "input-bucket": "{{ task.__class__.private_output_bucket }}",
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)

addon_aggregates_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="addon_aggregates_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="addons/agg",
        dataset_version="v2",
        gke_cluster_name="bq-load-gke-1",
        p2b_table_alias="addon_aggregates_v2",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="addon_aggregates_bigquery_load",
    dag=dag)

main_summary_experiments = MozDatabricksSubmitRunOperator(
    task_id="main_summary_experiments",
    job_name="Experiments Main Summary View",
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    max_instance_count=40,
    enable_autoscale=True,
    instance_type="i3.2xlarge",
    spot_bid_price_percent=50,
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
    env=tbv_envvar("com.mozilla.telemetry.views.ExperimentSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

main_summary_experiments_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="main_summary_experiments_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="experiments",
        dataset_version="v1",
        objects_prefix='experiments/v1',
        spark_gs_dataset_location='gs://moz-fx-data-derived-datasets-parquet-tmp/experiments/v1/*/submission_date_s3={{ds_nodash}}',
        gke_cluster_name="bq-load-gke-1",
        p2b_resume=True,
        reprocess=True,
        bigquery_dataset="telemetry_derived",
        cluster_by=["experiment_id"],
        drop=["submission_date"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="main_summary_experiments_bigquery_load",
    dag=dag)

experiments_aggregates_import = EMRSparkOperator(
    task_id="experiments_aggregates_import",
    job_name="Experiments Aggregates Import",
    execution_timeout=timedelta(hours=10),
    instance_count=1,
    disable_on_dev=True,
    owner="robhudson@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "robhudson@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/firefox-test-tube/master/notebook/import.py",
    dag=dag)

search_dashboard = EMRSparkOperator(
    task_id="search_dashboard",
    job_name="Search Dashboard",
    execution_timeout=timedelta(hours=3),
    instance_count=3,
    owner="harterrt@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "harterrt@mozilla.com", "wlachance@mozilla.com"],
    env=mozetl_envvar("search_dashboard", {
        "submission_date": "{{ ds_nodash }}",
        "input_bucket": "{{ task.__class__.private_output_bucket }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "harter/searchdb",
        "save_mode": "overwrite"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

search_dashboard_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="search_dashboard_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="harter/searchdb",
        dataset_version="v7",
        bigquery_dataset="search",
        gke_cluster_name="bq-load-gke-1",
        p2b_table_alias="search_aggregates_v7",
        ),
    task_id="search_dashboard_bigquery_load",
    dag=dag)

search_clients_daily = EMRSparkOperator(
    task_id="search_clients_daily",
    job_name="Search Clients Daily",
    execution_timeout=timedelta(hours=5),
    instance_count=5,
    owner="harterrt@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "harterrt@mozilla.com", "wlachance@mozilla.com"],
    env=mozetl_envvar("search_clients_daily", {
        "submission_date": "{{ ds_nodash }}",
        "input_bucket": "{{ task.__class__.private_output_bucket }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "search_clients_daily",
        "save_mode": "overwrite"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

search_clients_daily_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="search_clients_daily_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="search_clients_daily",
        dataset_version="v7",
        bigquery_dataset="search",
        gke_cluster_name="bq-load-gke-1",
        reprocess=True,
        ),
    task_id="search_clients_daily_bigquery_load",
    dag=dag)

clients_daily = bigquery_etl_query(
    task_id="clients_daily",
    destination_table="clients_daily_v6",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "pmcdermott@mozilla.com", "dzielaski@mozilla.com", "jmundi@mozilla.com"],
    start_date=datetime(2019, 11, 5),
    dag=dag)

clients_daily_export = SubDagOperator(
    subdag=export_to_parquet(
        table="moz-fx-data-shared-prod:telemetry_derived.clients_daily_v6${{ds_nodash}}",
        static_partitions="submission_date_s3={{ds_nodash}}",
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
    executor=GetDefaultExecutor(),
    dag=dag)

register_status(clients_daily, "Clients Daily", "A view of main pings with one row per client per day.")

clients_last_seen = bigquery_etl_query(
    task_id="clients_last_seen",
    destination_table="clients_last_seen_v1",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "jklukas@mozilla.com", "pmcdermott@mozilla.com", "dzielaski@mozilla.com", "jmundi@mozilla.com"],
    depends_on_past=True,
    start_date=datetime(2019, 4, 15),
    dag=dag)

clients_last_seen_export = SubDagOperator(
    subdag=export_to_parquet(
        table="moz-fx-data-shared-prod:telemetry_derived.clients_last_seen_v1${{ds_nodash}}",
        static_partitions="submission_date={{ds}}",
        arguments=[
            "--select",
            "cast(log2(days_seen_bits & -days_seen_bits) as long) as days_since_seen",
            "cast(log2(days_visited_5_uri_bits & -days_visited_5_uri_bits) as long) as days_since_visited_5_uri",
            "cast(log2(days_opened_dev_tools_bits & -days_opened_dev_tools_bits) as long) as days_since_opened_dev_tools",
            "cast(log2(days_created_profile_bits & -days_created_profile_bits) as long) as days_since_created_profile",
            "*",
            # restore legacy schema
            "--replace",
            "STRUCT(TRANSFORM(active_addons, element -> STRUCT(element)) AS list) AS active_addons",
            "STRUCT(TRANSFORM(environment_settings_intl_accept_languages, element -> STRUCT(element)) AS list) AS environment_settings_intl_accept_languages",
            "STRUCT(TRANSFORM(environment_settings_intl_app_locales, element -> STRUCT(element)) AS list) AS environment_settings_intl_app_locales",
            "STRUCT(TRANSFORM(environment_settings_intl_available_locales, element -> STRUCT(element)) AS list) AS environment_settings_intl_available_locales",
            "STRUCT(TRANSFORM(environment_settings_intl_regional_prefs_locales, element -> STRUCT(element)) AS list) AS environment_settings_intl_regional_prefs_locales",
            "STRUCT(TRANSFORM(environment_settings_intl_requested_locales, element -> STRUCT(element)) AS list) AS environment_settings_intl_requested_locales",
            "STRUCT(TRANSFORM(environment_settings_intl_system_locales, element -> STRUCT(element)) AS list) AS environment_settings_intl_system_locales",
            "STRUCT(experiments AS key_value) AS experiments",
            "STRUCT(scalar_parent_devtools_accessibility_select_accessible_for_node_sum AS key_value) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum",
        ],
        parent_dag_name=dag.dag_id,
        dag_name="clients_last_seen_export",
        default_args=default_args,
        num_preemptible_workers=10),
    task_id="clients_last_seen_export",
    executor=GetDefaultExecutor(),
    dag=dag)

exact_mau_by_dimensions = bigquery_etl_query(
    task_id="exact_mau_by_dimensions",
    destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
    dataset_id="telemetry",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "pmcdermott@mozilla.com", "dzielaski@mozilla.com", "jmundi@mozilla.com"],
    dag=dag)

exact_mau_by_dimensions_export = SubDagOperator(
    subdag=export_to_parquet(
        table="telemetry.firefox_desktop_exact_mau28_by_dimensions_v1${{ds_nodash}}",
        static_partitions="submission_date={{ds}}",
        parent_dag_name=dag.dag_id,
        dag_name="exact_mau_by_dimensions_export",
        default_args=default_args),
    task_id="exact_mau_by_dimensions_export",
    executor=GetDefaultExecutor(),
    dag=dag)

smoot_usage_desktop_v2 = bigquery_etl_query(
    task_id='smoot_usage_desktop_v2',
    project_id='moz-fx-data-shared-prod',
    destination_table='smoot_usage_desktop_v2',
    dataset_id='telemetry_derived',
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

main_summary_glue = EMRSparkOperator(
    task_id="main_summary_glue",
    job_name="Main Summary Update Glue",
    execution_timeout=timedelta(hours=8),
    owner="bimsland@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bimsland@mozilla.com"],
    instance_count=1,
    disable_on_dev=True,
    env={
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "main_summary",
        "pdsm_version": "{{ var.value.pdsm_version }}",
        "glue_access_key_id": "{{ var.value.glue_access_key_id }}",
        "glue_secret_access_key": "{{ var.value.glue_secret_access_key }}",
        "glue_default_region": "{{ var.value.glue_default_region }}",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/update_glue.sh",
    dag=dag)

taar_dynamo = EMRSparkOperator(
    task_id="taar_dynamo",
    job_name="TAAR DynamoDB loader",
    execution_timeout=timedelta(hours=14),
    instance_count=6,
    disable_on_dev=True,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "sbird@mozilla.com"],
    env=mozetl_envvar("taar_dynamo", {
        "date": "{{ ds_nodash }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)


taar_locale_job = SubDagOperator(
    task_id="taar_locale_job",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_locale_job",
        default_args=default_args,
        cluster_name=taar_locale_cluster_name,
        job_name="TAAR_Locale",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_locale.py",
        num_workers=12,
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
            "--bucket",
            "telemetry-private-analysis-2",
            "--prefix",
            "taar/locale/",
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
    ),
    dag=dag,
)


taar_similarity = MozDatabricksSubmitRunOperator(
    task_id="taar_similarity",
    job_name="Taar Similarity model",
    owner="akomar@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com", "akomar@mozilla.com"],
    execution_timeout=timedelta(hours=2),
    instance_count=11,
    instance_type="i3.8xlarge",
    driver_instance_type="i3.xlarge",
    env=mozetl_envvar("taar_similarity",
        options={
            "date": "{{ ds_nodash }}",
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "prefix": "taar/similarity/"
        }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

taar_collaborative_recommender = SubDagOperator(
    task_id="addon_recommender",
    subdag=moz_dataproc_jar_runner(
        parent_dag_name=dag.dag_id,
        dag_name="addon_recommender",
        job_name="Train the Collaborative Addon Recommender",
        main_class="com.mozilla.telemetry.ml.AddonRecommender",
        jar_urls=[
            "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-ci-artifacts"
            "/mozilla/telemetry-batch-view/master/telemetry-batch-view.jar",
        ],
        jar_args=[
          "--runDate={{ds_nodash}}",
          "--inputTable=gs://moz-fx-data-derived-datasets-parquet/clients_daily/v6",
          "--privateBucket=telemetry-parquet",
          "--publicBucket=telemetry-public-analysis-2",
        ],
        cluster_name="addon-recommender-{{ds_nodash}}",
        image_version="1.3",
        worker_machine_type="n1-standard-8",
        num_workers=20,
        optional_components=[],
        install_component_gateway=False,
        init_actions_uris=[],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
        default_args={
            key: value
            for key, value in chain(default_args.items(), [
                ("owner", "mlopatka@mozilla.com"),
                ("email", ["telemetry-alerts@mozilla.com", "mlopatka@mozilla.com", "vng@mozilla.com"]),
            ])
        },
    ),
    dag=dag,
)

bgbb_pred = MozDatabricksSubmitRunOperator(
    task_id="bgbb_pred",
    job_name="Predict retention from a BGBB model",
    execution_timeout=timedelta(hours=2),
    email=[
        "telemetry-alerts@mozilla.com",
        "wbeard@mozilla.com",
        "amiyaguchi@mozilla.com",
    ],
    instance_count=10,
    release_label="6.1.x-scala2.11",
    env=mozetl_envvar(
        "bgbb_pred",
        {
            "submission-date": "{{ ds }}",
            "model-win": "90",
            "sample-ids": "[]",
            "param-bucket": "{{ task.__class__.private_output_bucket }}",
            "param-prefix": "bgbb/params/v1",
            "pred-bucket": "{{ task.__class__.private_output_bucket }}",
            "pred-prefix": "bgbb/active_profiles/v1",
        },
        dev_options={
            "model-win": "30",
            "sample-ids": "[1]",
        },
        other={
            "MOZETL_GIT_PATH": "https://github.com/wcbeard/bgbb_airflow.git",
            "MOZETL_EXTERNAL_MODULE": "bgbb_airflow",
        },
    ),
    dag=dag
)

bgbb_pred_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="bgbb_pred_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="bgbb/active_profiles",
        dataset_version="v1",
        p2b_table_alias="active_profiles_v1",
        bigquery_dataset="telemetry_derived",
        ds_type="ds",
        gke_cluster_name="bq-load-gke-1",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="bgbb_pred_bigquery_load",
    dag=dag)

search_clients_daily_bigquery = bigquery_etl_query(
    task_id="search_clients_daily_bigquery",
    destination_table="search_clients_daily_v8",
    dataset_id="search_derived",
    project_id="moz-fx-data-shared-prod",
    owner="bewu@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bewu@mozilla.com"],
    dag=dag)

search_aggregates_bigquery = bigquery_etl_query(
    task_id="search_aggregates_bigquery",
    destination_table="search_aggregates_v8",
    dataset_id="search_derived",
    project_id="moz-fx-data-shared-prod",
    owner="bewu@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bewu@mozilla.com"],
    dag=dag)

taar_lite = SubDagOperator(
    task_id="taar_lite",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_lite",
        default_args=default_args,
        cluster_name=taarlite_cluster_name,
        job_name="TAAR_Lite_GUID_GUID",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_lite_guidguid.py",
        num_workers=8,
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
    ),
    dag=dag,
)


main_summary.set_upstream(copy_deduplicate_main_ping)
main_summary_export.set_upstream(main_summary)
clients_daily.set_upstream(main_summary)
clients_daily_export.set_upstream(clients_daily)

addons.set_upstream(main_summary_export)
addons_bigquery_load.set_upstream(addons)
addon_aggregates.set_upstream(addons)
addon_aggregates_bigquery_load.set_upstream(addon_aggregates)

main_events.set_upstream(main_summary_export)
main_events_bigquery_load.set_upstream(main_events)

main_summary_experiments.set_upstream(main_summary_export)
main_summary_experiments_bigquery_load.set_upstream(main_summary_experiments)

experiments_aggregates_import.set_upstream(main_summary_experiments)
search_dashboard.set_upstream(main_summary_export)
search_dashboard_bigquery_load.set_upstream(search_dashboard)
search_clients_daily.set_upstream(main_summary_export)
search_clients_daily_bigquery_load.set_upstream(search_clients_daily)

taar_dynamo.set_upstream(main_summary_export)
taar_similarity.set_upstream(clients_daily_export)

clients_last_seen.set_upstream(clients_daily)
clients_last_seen_export.set_upstream(clients_last_seen)
exact_mau_by_dimensions.set_upstream(clients_last_seen)
exact_mau_by_dimensions_export.set_upstream(exact_mau_by_dimensions)
smoot_usage_desktop_v2.set_upstream(clients_last_seen)

main_summary_glue.set_upstream(main_summary_export)

taar_locale_job.set_upstream(clients_daily_export)
taar_collaborative_recommender.set_upstream(clients_daily_export)

bgbb_pred.set_upstream(clients_daily_export)
bgbb_pred_bigquery_load.set_upstream(bgbb_pred)

search_clients_daily_bigquery.set_upstream(main_summary)
search_aggregates_bigquery.set_upstream(search_clients_daily_bigquery)

# Set a dependency on clients_daily from taar_lite
taar_lite.set_upstream(clients_daily_export)

bq_main_events.set_upstream(copy_deduplicate_main_ping)
