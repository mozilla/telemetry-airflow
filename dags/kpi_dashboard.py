import datetime

from airflow import models
from utils.gcp import bigquery_etl_query

default_args = {
    'owner': 'jklukas@mozilla.com',
    'start_date': datetime.datetime(2019, 5, 12),
    'email': ['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag_name = 'kpi_dashboard'

with models.DAG(
        dag_name,
        # KPI dashboard refreshes at 16:00 UTC, so run this 15 minutes beforehand.
        schedule_interval='45 15 * * *',
        default_args=default_args) as dag:

    kpi_dashboard = bigquery_etl_query(
        destination_table='firefox_kpi_dashboard_v1',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
        date_partition_parameter=None,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com']
    )

    smoot_usage_new_profiles_v2 = bigquery_etl_query(
        task_id='smoot_usage_new_profiles_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_new_profiles_v2',
        dataset_id='telemetry_derived',
    )

    smoot_usage_new_profiles_compressed_v2 = bigquery_etl_query(
        task_id='smoot_usage_new_profiles_compressed_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_new_profiles_compressed_v2',
        dataset_id='telemetry_derived',
    )

    smoot_usage_new_profiles_v2 >> smoot_usage_new_profiles_compressed_v2
