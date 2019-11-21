import datetime

from airflow import models
from utils.gcp import bigquery_etl_copy_deduplicate, bigquery_etl_query

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
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    copy_deduplicate_all >> event_events


    # Daily and last seen views on top of core pings.

    core_clients_daily = bigquery_etl_query(
        task_id='core_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='core_clients_daily_v1',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    core_clients_last_seen = bigquery_etl_query(
        task_id='core_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='core_clients_last_seen_v1',
        dataset_id='telemetry_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    # Daily and last seen views on top of glean pings.

    fenix_clients_daily = bigquery_etl_query(
        task_id='fenix_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_daily_v1',
        dataset_id='org_mozilla_fenix_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    fenix_clients_last_seen = bigquery_etl_query(
        task_id='fenix_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_last_seen_v1',
        dataset_id='org_mozilla_fenix_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    # Aggregated nondesktop tables and their dependency chains.

    firefox_nondesktop_exact_mau28 = bigquery_etl_query(
        task_id='firefox_nondesktop_exact_mau28',
        destination_table='firefox_nondesktop_exact_mau28_raw_v1',
        dataset_id='telemetry',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    smoot_usage_nondesktop_v2 = bigquery_etl_query(
        task_id='smoot_usage_nondesktop_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_nondesktop_v2',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    (copy_deduplicate_all >>
     core_clients_daily >>
     core_clients_last_seen >>
     [firefox_nondesktop_exact_mau28, smoot_usage_nondesktop_v2])

    (copy_deduplicate_all >>
     fenix_clients_daily >>
     fenix_clients_last_seen >>
     [firefox_nondesktop_exact_mau28, smoot_usage_nondesktop_v2])
