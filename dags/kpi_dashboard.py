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
        dataset_id='telemetry',
        date_partition_parameter=None,
    )

    smoot_usage_all_mtr = bigquery_etl_query(
        destination_table='smoot_usage_all_mtr_v1',
        dataset_id='telemetry',
        date_partition_parameter=None,
    )

    smoot_usage_new_profiles_v2 = bigquery_etl_query(
        task_id='smoot_usage_new_profiles_v2',
        destination_table='moz-fx-data-shared-prod:telemetry_derived.smoot_usage_new_profiles_v2',
        sql_file_path='sql/telemetry_derived/smoot_usage_new_profiles_v2/query.sql',
        dataset_id='telemetry_derived',
    )
