import datetime

from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from utils.forecasting import simpleprophet_forecast
from utils.gcp import (bigquery_etl_copy_deduplicate,
                       bigquery_etl_query,
                       gke_command,
                       bigquery_xcom_query)

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
        dag_name,
        schedule_interval="0 1 * * *",
        default_args=default_args) as dag:

    # This single task is responsible for sequentially running copy queries
    # over all the tables in _live datasets into _stable datasets except those
    # that are specifically used in another DAG.
    copy_deduplicate_all = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_all",
        target_project_id="moz-fx-data-shared-prod",
        # Any table listed here under except_tables _must_ have a corresponding
        # copy_deduplicate job in another DAG.
        except_tables=["telemetry_live.main_v4"])


    # Events.

    event_events = bigquery_etl_query(
        task_id="event_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="event_events_v1",
        dataset_id="telemetry_derived",
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    copy_deduplicate_all >> event_events

    # Experiment enrollment aggregates chain (depends on events)

    wait_for_main_events = ExternalTaskSensor(
        task_id="wait_for_main_events",
        external_dag_id="main_summary",
        external_task_id="bq_main_events",
        dag=dag)


    experiment_enrollment_aggregates = bigquery_etl_query(
        task_id="experiment_enrollment_aggregates",
        project_id="moz-fx-data-shared-prod",
        destination_table="experiment_enrollment_aggregates_v1",
        dataset_id="telemetry_derived",
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    gen_query_task_id = "experiment_enrollment_aggregates_live_generate_query"

    # setting xcom_push to True outputs this query to an xcom
    experiment_enrollment_aggregates_live_generate_query = gke_command(
        task_id=gen_query_task_id,
        command=[
            "python",
            "sql/telemetry_derived/experiment_enrollment_aggregates_live/view.sql.py",
            "--submission-date",
            "{{ ds }}",
            "--json-output",
            "--wait-seconds",
            "15",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        xcom_push=True,
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    experiment_enrollment_aggregates_live_run_query = bigquery_xcom_query(
        task_id="experiment_enrollment_aggregates_live_run_query",
        destination_table=None,
        dataset_id="telemetry_derived",
        xcom_task_id=gen_query_task_id,
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    wait_for_main_events >> experiment_enrollment_aggregates
    (event_events >>
     experiment_enrollment_aggregates >>
     experiment_enrollment_aggregates_live_generate_query >>
     experiment_enrollment_aggregates_live_run_query)

    # Derived tables for activity-stream.

    impression_stats_flat = bigquery_etl_query(
        task_id='impression_stats_flat',
        project_id='moz-fx-data-shared-prod',
        destination_table='impression_stats_flat_v1',
        dataset_id='activity_stream_bi',
        email=['jklukas@mozilla.com'],
    )

    copy_deduplicate_all >> impression_stats_flat

    # Derived tables for messaging-system.

    # CFR
    messaging_system_cfr_users_daily = bigquery_etl_query(
        task_id='messaging_system_cfr_users_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='cfr_users_daily_v1',
        dataset_id='messaging_system_derived',
        email=['najiang@mozilla.com'],
    )

    messaging_system_cfr_users_last_seen = bigquery_etl_query(
        task_id='messaging_system_cfr_users_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='cfr_users_last_seen_v1',
        dataset_id='messaging_system_derived',
        depends_on_past=True,
        email=['najiang@mozilla.com'],
    )

    messaging_system_cfr_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id='messaging_system_cfr_exact_mau28_by_dimensions',
        project_id='moz-fx-data-shared-prod',
        destination_table='cfr_exact_mau28_by_dimensions_v1',
        dataset_id='messaging_system_derived',
        email=['najiang@mozilla.com'],
    )

    (copy_deduplicate_all >>
     messaging_system_cfr_users_daily >>
     messaging_system_cfr_users_last_seen >>
     messaging_system_cfr_exact_mau28_by_dimensions)

    # Onboarding
    messaging_system_onboarding_users_daily = bigquery_etl_query(
        task_id='messaging_system_onboarding_users_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='onboarding_users_daily_v1',
        dataset_id='messaging_system_derived',
        email=['najiang@mozilla.com'],
    )

    messaging_system_onboarding_users_last_seen = bigquery_etl_query(
        task_id='messaging_system_onboarding_users_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='onboarding_users_last_seen_v1',
        dataset_id='messaging_system_derived',
        depends_on_past=True,
        email=['najiang@mozilla.com'],
    )

    messaging_system_onboarding_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id='messaging_system_onboarding_exact_mau28_by_dimensions',
        project_id='moz-fx-data-shared-prod',
        destination_table='onboarding_exact_mau28_by_dimensions_v1',
        dataset_id='messaging_system_derived',
        email=['najiang@mozilla.com'],
    )

    (copy_deduplicate_all >>
     messaging_system_onboarding_users_daily >>
     messaging_system_onboarding_users_last_seen >>
     messaging_system_onboarding_exact_mau28_by_dimensions)

    # Snippets
    messaging_system_snippets_users_daily = bigquery_etl_query(
        task_id='messaging_system_snippets_users_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='snippets_users_daily_v1',
        dataset_id='messaging_system_derived',
        email=['najiang@mozilla.com'],
    )

    messaging_system_snippets_users_last_seen = bigquery_etl_query(
        task_id='messaging_system_snippets_users_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='snippets_users_last_seen_v1',
        dataset_id='messaging_system_derived',
        depends_on_past=True,
        email=['najiang@mozilla.com'],
    )

    messaging_system_snippets_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id='messaging_system_snippets_exact_mau28_by_dimensions',
        project_id='moz-fx-data-shared-prod',
        destination_table='snippets_exact_mau28_by_dimensions_v1',
        dataset_id='messaging_system_derived',
        email=['najiang@mozilla.com'],
    )

    (copy_deduplicate_all >>
     messaging_system_snippets_users_daily >>
     messaging_system_snippets_users_last_seen >>
     messaging_system_snippets_exact_mau28_by_dimensions)

    # Daily and last seen views on top of core pings.

    core_clients_daily = bigquery_etl_query(
        task_id='core_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='core_clients_daily_v1',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    core_clients_last_seen = bigquery_etl_query(
        task_id='core_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='core_clients_last_seen_v1',
        dataset_id='telemetry_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    # Daily and last seen views on top of Fenix pings (deprecated);
    # these legacy tables consider both baseline and metrics pings as activity
    # and should be removed once GUD and KPI reporting consistently use the
    # new org_mozilla_firefox tables.

    fenix_clients_daily = bigquery_etl_query(
        task_id='fenix_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_daily_v1',
        dataset_id='org_mozilla_fenix_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    fenix_clients_last_seen = bigquery_etl_query(
        task_id='fenix_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_last_seen_v1',
        dataset_id='org_mozilla_fenix_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    # Daily and last seen views on top of Fenix pings;
    # these will replace the above

    org_mozilla_firefox_baseline_daily = bigquery_etl_query(
        task_id='org_mozilla_firefox_baseline_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='baseline_daily_v1',
        dataset_id='org_mozilla_firefox_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    org_mozilla_firefox_metrics_daily = bigquery_etl_query(
        task_id='org_mozilla_firefox_metrics_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='metrics_daily_v1',
        dataset_id='org_mozilla_firefox_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    org_mozilla_firefox_clients_last_seen = bigquery_etl_query(
        task_id='org_mozilla_firefox_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_last_seen_v1',
        dataset_id='org_mozilla_firefox_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    # Daily and last seen views on top of VR browser pings.

    vrbrowser_baseline_daily = bigquery_etl_query(
        task_id='vrbrowser_baseline_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='baseline_daily_v1',
        dataset_id='org_mozilla_vrbrowser_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'ascholtz@mozilla.com'],
    )

    vrbrowser_metrics_daily = bigquery_etl_query(
        task_id='vrbrowser_metrics_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='metrics_daily_v1',
        dataset_id='org_mozilla_vrbrowser_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'ascholtz@mozilla.com'],
    )

    vrbrowser_clients_daily = bigquery_etl_query(
        task_id='vrbrowser_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_daily_v1',
        dataset_id='org_mozilla_vrbrowser_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'ascholtz@mozilla.com'],
    )

    vrbrowser_clients_last_seen = bigquery_etl_query(
        task_id='vrbrowser_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_last_seen_v1',
        dataset_id='org_mozilla_vrbrowser_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'ascholtz@mozilla.com'],
    )

    # Aggregated nondesktop tables and their dependency chains.

    firefox_nondesktop_exact_mau28 = bigquery_etl_query(
        task_id='firefox_nondesktop_exact_mau28',
        project_id='moz-fx-data-shared-prod',
        destination_table='firefox_nondesktop_exact_mau28_raw_v1',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    smoot_usage_nondesktop_v2 = bigquery_etl_query(
        task_id='smoot_usage_nondesktop_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_nondesktop_v2',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    smoot_usage_nondesktop_compressed_v2 = bigquery_etl_query(
        task_id='smoot_usage_nondesktop_compressed_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_nondesktop_compressed_v2',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    smoot_usage_nondesktop_v2 >> smoot_usage_nondesktop_compressed_v2

    firefox_nondesktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id='firefox_nondesktop_exact_mau28_by_client_count_dimensions',
        project_id='moz-fx-data-shared-prod',
        destination_table='firefox_nondesktop_exact_mau28_by_client_count_dimensions_v1',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    nondesktop_aggregate_tasks = [
        firefox_nondesktop_exact_mau28,
        smoot_usage_nondesktop_v2,
        firefox_nondesktop_exact_mau28_by_client_count_dimensions,
    ]

    (copy_deduplicate_all >>
     core_clients_daily >>
     core_clients_last_seen >>
     nondesktop_aggregate_tasks)

    # TODO: Remove this dependency chain once we retire fenix_* tasks.
    (copy_deduplicate_all >>
     fenix_clients_daily >>
     fenix_clients_last_seen >>
     nondesktop_aggregate_tasks)

    (copy_deduplicate_all >>
     [org_mozilla_firefox_baseline_daily, org_mozilla_firefox_metrics_daily] >>
     org_mozilla_firefox_clients_last_seen >>
     nondesktop_aggregate_tasks)

    (copy_deduplicate_all >>
     [vrbrowser_baseline_daily, vrbrowser_metrics_daily] >>
     vrbrowser_clients_daily >>
     vrbrowser_clients_last_seen >>
     nondesktop_aggregate_tasks)

    # Nondesktop forecasts.

    simpleprophet_forecasts_mobile = simpleprophet_forecast(
        task_id="simpleprophet_forecasts_mobile",
        datasource="mobile",
        project_id='moz-fx-data-shared-prod',
        dataset_id='telemetry_derived',
        table_id='simpleprophet_forecasts_mobile_v1',
        owner="jklukas@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    )

    firefox_nondesktop_exact_mau28 >> simpleprophet_forecasts_mobile

    # Mobile search queries and dependency chain.

    mobile_search_clients_daily = bigquery_etl_query(
        task_id='mobile_search_clients_daily',
        project_id='moz-fx-data-shared-prod',
        dataset_id="search_derived",
        destination_table='mobile_search_clients_daily_v1',
        email=['telemetry-alerts@mozilla.com', 'bewu@mozilla.com'],
    )

    mobile_search_aggregates = bigquery_etl_query(
        task_id='mobile_search_aggregates',
        project_id='moz-fx-data-shared-prod',
        dataset_id="search_derived",
        destination_table='mobile_search_aggregates_v1',
        email=['telemetry-alerts@mozilla.com', 'bewu@mozilla.com'],
    )

    (copy_deduplicate_all >>
     mobile_search_clients_daily >>
     mobile_search_aggregates)
