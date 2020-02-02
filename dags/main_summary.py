from airflow import DAG
from datetime import datetime, timedelta
from itertools import chain
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.executors import GetDefaultExecutor
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
from utils.status import register_status
from utils.gcp import (
    bigquery_etl_query,
    bigquery_etl_copy_deduplicate,
    export_to_parquet,
    load_to_bigquery,
    gke_command,
)
from utils.forecasting import simpleprophet_forecast

taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(taar_aws_conn_id).get_credentials()
taarlite_cluster_name = "dataproc-taarlite-guidguid"
taar_locale_cluster_name = "dataproc-taar-locale"
taar_similarity_cluster_name = "dataproc-taar-similarity"
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"
taar_dynamo_cluster_name = "dataproc-taar-dynamo"

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
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "pmcdermott@mozilla.com", "dzielaski@mozilla.com", "jmundi@mozilla.com"],
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
    executor=GetDefaultExecutor(),
    dag=dag)

register_status(main_summary, "Main Summary", "A summary view of main pings.")

addons = bigquery_etl_query(
    task_id="addons",
    destination_table="addons_v2",
    dataset_id="telemetry_derived",
    dag=dag)

addon_aggregates = bigquery_etl_query(
    task_id="addon_aggregates",
    destination_table="addon_aggregates_v2",
    dataset_id="telemetry_derived",
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    dag=dag)

main_summary_experiments_get_experiment_list = gke_command(
    task_id="main_summary_experiments_get_experiment_list",
    command=["python3", "templates/telemetry_derived/experiments_v1/get_experiment_list.py", "{{ds}}"],
    docker_image="mozilla/bigquery-etl:latest",
    xcom_push=True,
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
    dag=dag)

main_summary_experiments = bigquery_etl_query(
    task_id="main_summary_experiments",
    destination_table="experiments_v1",
    parameters=(
        "experiment_list:ARRAY<STRING>:{{task_instance.xcom_pull('main_summary_experiments_get_experiment_list') | tojson}}",
    ),
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
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

exact_mau_by_dimensions = bigquery_etl_query(
    task_id="exact_mau_by_dimensions",
    destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
    dataset_id="telemetry",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "pmcdermott@mozilla.com", "dzielaski@mozilla.com", "jmundi@mozilla.com"],
    dag=dag)

exact_mau_by_client_count_dimensions = bigquery_etl_query(
    task_id="exact_mau_by_client_count_dimensions",
    project_id='moz-fx-data-shared-prod',
    destination_table="firefox_desktop_exact_mau28_by_client_count_dimensions_v1",
    dataset_id="telemetry_derived",
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

smoot_usage_desktop_v2 = bigquery_etl_query(
    task_id='smoot_usage_desktop_v2',
    project_id='moz-fx-data-shared-prod',
    destination_table='smoot_usage_desktop_v2',
    dataset_id='telemetry_derived',
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

smoot_usage_desktop_compressed_v2 = bigquery_etl_query(
    task_id='smoot_usage_desktop_compressed_v2',
    project_id='moz-fx-data-shared-prod',
    destination_table='smoot_usage_desktop_compressed_v2',
    dataset_id='telemetry_derived',
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

simpleprophet_forecasts_desktop = simpleprophet_forecast(
    task_id="simpleprophet_forecasts_desktop",
    datasource="desktop",
    project_id='moz-fx-data-shared-prod',
    dataset_id='telemetry_derived',
    table_id='simpleprophet_forecasts_desktop_v1',
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


taar_dynamo_job = SubDagOperator(
    task_id="taar_dynamo_job",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_dynamo_job",
        default_args=default_args,
        master_machine_type='n1-standard-32',
        worker_machine_type='n1-standard-32',
        cluster_name=taar_dynamo_cluster_name,
        job_name="TAAR_Dynamo",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_dynamo.py",
        num_workers=12,
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
        master_disk_type='pd-ssd',
        worker_disk_type='pd-ssd',
    ),
    dag=dag,
)



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

taar_similarity_args = default_args.copy()
taar_similarity_args["owner"] = "vng@mozilla.com"
taar_similarity_args["email"] = ["vng@mozilla.com", "mlopatka@mozilla.com", "akomar@mozilla.com"]
taar_similarity = SubDagOperator(
    task_id="taar_similarity",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_similarity",
        default_args=taar_similarity_args,
        cluster_name=taar_similarity_cluster_name,
        job_name="TAAR_similarity",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_similarity.py",
        num_workers=4,
        worker_machine_type="n1-highmem-96",
        master_machine_type='n1-standard-8',
        py_args=[
            "--date", "{{ ds_nodash }}",
            "--bucket", "telemetry-private-analysis-2",
            "--prefix", "taar/similarity/",
            "--aws_access_key_id", taar_aws_access_key,
            "--aws_secret_access_key", taar_aws_secret_key,
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
    ),
    dag=dag,
)

taar_collaborative_recommender = SubDagOperator(
    task_id="addon_recommender",
    subdag=moz_dataproc_jar_runner(
        parent_dag_name=dag.dag_id,
        dag_name="addon_recommender",
        job_name="Train_the_Collaborative_Addon_Recommender",
        main_class="com.mozilla.telemetry.ml.AddonRecommender",
        jar_urls=[
            "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-ci-artifacts"
            "/mozilla/telemetry-batch-view/master/telemetry-batch-view.jar",
        ],
        jar_args=[
          "train",
          "--runDate={{ds_nodash}}",
          "--inputTable=gs://moz-fx-data-derived-datasets-parquet/clients_daily/v6",
          "--privateBucket=s3a://telemetry-parquet",
          "--publicBucket=s3a://telemetry-public-analysis-2",
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

subdag_args = default_args.copy()
subdag_args["retries"] = 0
task_id = "bgbb_pred_dataproc"
params = get_dataproc_parameters("google_cloud_airflow_dataproc")

bgbb_pred_dataproc = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="bgbb_pred_dataproc",
        cluster_name="bgbb-pred-{{ ds_nodash }}",
        idle_delete_ttl="600",
        num_workers=10,
        worker_machine_type="n1-standard-8",
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        },
        additional_metadata={
            "PIP_PACKAGES": "git+https://github.com/wcbeard/bgbb_airflow.git"
        },
        python_driver_code="gs://{}/jobs/bgbb_runner.py".format(params.artifact_bucket),
        py_args=[
            "bgbb_pred",
            "--submission-date",
            "{{ ds }}",
            "--model-win",
            "90",
            "--sample-ids",
            "[42]" if params.is_dev else "[]",
            "--source",
            "bigquery",
            "--view-materialization-project",
            params.project_id if params.is_dev else "moz-fx-data-shared-prod",
            "--view-materialization-dataset",
            "analysis",
            "--bucket-protocol",
            "gs",
            "--param-bucket",
            params.output_bucket,
            "--param-prefix",
            "bgbb/params/v1",
            "--pred-bucket",
            params.output_bucket,
            "--pred-prefix",
            "bgbb/active_profiles/v1",
        ],
        gcp_conn_id=params.conn_id,
        service_account=params.client_email,
        artifact_bucket=params.artifact_bucket,
        storage_bucket=params.storage_bucket,
        default_args=subdag_args,
    ),
)

bgbb_pred_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="bgbb_pred_bigquery_load",
        default_args=default_args,
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

search_clients_last_seen = bigquery_etl_query(
    task_id="search_clients_last_seen",
    destination_table="search_clients_last_seen_v1",
    dataset_id="search_derived",
    project_id="moz-fx-data-shared-prod",
    depends_on_past=True,
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
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

main_summary_experiments.set_upstream(main_summary)
main_summary_experiments.set_upstream(main_summary_experiments_get_experiment_list)

taar_dynamo_job.set_upstream(main_summary_export)
taar_similarity.set_upstream(clients_daily_export)

clients_last_seen.set_upstream(clients_daily)
exact_mau_by_dimensions.set_upstream(clients_last_seen)
exact_mau_by_client_count_dimensions.set_upstream(clients_last_seen)
smoot_usage_desktop_v2.set_upstream(clients_last_seen)
smoot_usage_desktop_compressed_v2.set_upstream(smoot_usage_desktop_v2)
simpleprophet_forecasts_desktop.set_upstream(exact_mau_by_dimensions)
devtools_panel_usage.set_upstream(clients_daily)

taar_locale_job.set_upstream(clients_daily_export)
taar_collaborative_recommender.set_upstream(clients_daily_export)

bgbb_pred_dataproc.set_upstream(clients_daily)
bgbb_pred_bigquery_load.set_upstream(bgbb_pred_dataproc)

search_clients_daily_bigquery.set_upstream(main_summary)
search_aggregates_bigquery.set_upstream(search_clients_daily_bigquery)
search_clients_last_seen.set_upstream(search_clients_daily_bigquery)

# Set a dependency on clients_daily from taar_lite
taar_lite.set_upstream(clients_daily_export)

bq_main_events.set_upstream(copy_deduplicate_main_ping)

experiments_daily_active_clients.set_upstream(clients_daily)
