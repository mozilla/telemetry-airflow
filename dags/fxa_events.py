import datetime

from airflow import models
from utils.forecasting import simpleprophet_forecast
from utils.gcp import bigquery_etl_query

default_args = {
    'owner': 'jklukas@mozilla.com',
    'start_date': datetime.datetime(2019, 3, 1),
    'email': ['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag_name = 'fxa_events'

with models.DAG(
        dag_name,
        # Continue to run DAG once per day
        schedule_interval='0 10 * * *',
        default_args=default_args) as dag:

    fxa_auth_events = bigquery_etl_query(
        task_id='fxa_auth_events',
        destination_table='fxa_auth_events_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    fxa_auth_bounce_events = bigquery_etl_query(
        task_id='fxa_auth_bounce_events',
        destination_table='fxa_auth_bounce_events_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    fxa_content_events = bigquery_etl_query(
        task_id='fxa_content_events',
        destination_table='fxa_content_events_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    fxa_users_daily = bigquery_etl_query(
        task_id='fxa_users_daily',
        destination_table='fxa_users_daily_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
    )

    fxa_users_daily << fxa_auth_events
    fxa_users_daily << fxa_auth_bounce_events
    fxa_users_daily << fxa_content_events

    fxa_users_last_seen = bigquery_etl_query(
        task_id='fxa_users_last_seen',
        destination_table='fxa_users_last_seen_raw_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
        depends_on_past=True,
        start_date=datetime.datetime(2019, 4, 23),
    )

    fxa_users_daily >> fxa_users_last_seen

    firefox_accounts_exact_mau28_raw = bigquery_etl_query(
        task_id='firefox_accounts_exact_mau28_raw',
        destination_table='firefox_accounts_exact_mau28_raw_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
    )

    fxa_users_last_seen >> firefox_accounts_exact_mau28_raw

    smoot_usage_fxa_v2 = bigquery_etl_query(
        task_id='smoot_usage_fxa_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_fxa_v2',
        dataset_id='telemetry_derived',
    )

    smoot_usage_fxa_compressed_v2 = bigquery_etl_query(
        task_id='smoot_usage_fxa_compressed_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_fxa_compressed_v2',
        dataset_id='telemetry_derived',
    )

    fxa_users_last_seen >> smoot_usage_fxa_v2 >> smoot_usage_fxa_compressed_v2

    simpleprophet_forecasts_fxa = simpleprophet_forecast(
        task_id="fxa_simpleprophet_forecasts",
        datasource="fxa",
        project_id='moz-fx-data-shared-prod',
        dataset_id='telemetry_derived',
        table_id='simpleprophet_forecasts_fxa_v1',
        email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    )

    simpleprophet_forecasts_fxa << firefox_accounts_exact_mau28_raw


    # Per-user-per-service tables.

    fxa_users_services_daily = bigquery_etl_query(
        task_id='fxa_users_services_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='fxa_users_services_daily_v1',
        dataset_id='telemetry_derived',
    )

    fxa_users_services_daily << fxa_auth_events
    fxa_users_services_daily << fxa_auth_bounce_events
    fxa_users_services_daily << fxa_content_events

    fxa_users_services_first_seen = bigquery_etl_query(
        task_id='fxa_users_services_first_seen',
        project_id='moz-fx-data-shared-prod',
        sql_file_path='sql/telemetry_derived/fxa_users_services_first_seen_v1/init.sql',
        dataset_id='telemetry_derived',
        # At least for now, we completely recreate this table every day;
        # making it incremental is possible but nuanced since it windows over
        # events that may cross the midnight boundary.
        date_partition_parameter=None,
        # The init.sql file contains a CREATE TABLE statement, so we must not
        # set a destination table or the query will return an error.
        destination_table=None,
    )

    fxa_users_services_first_seen << fxa_auth_events
    fxa_users_services_first_seen << fxa_auth_bounce_events
    fxa_users_services_first_seen << fxa_content_events

    fxa_users_services_last_seen = bigquery_etl_query(
        task_id='fxa_users_services_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='fxa_users_services_last_seen_v1',
        dataset_id='telemetry_derived',
        depends_on_past=True,
        start_date=datetime.datetime(2019, 10, 8),
    )

    fxa_users_services_daily >> fxa_users_services_last_seen
    fxa_users_services_first_seen >> fxa_users_services_last_seen
