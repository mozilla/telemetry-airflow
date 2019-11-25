import datetime

from airflow import models
from utils.gcp import bigquery_etl_query

default_args = {
    'owner': 'bewu@mozilla.com',
    'start_date': datetime.datetime(2019, 11, 1),
    'email': ['telemetry-alerts@mozilla.com', 'bewu@mozilla.com', 'wlachance@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=20),
}

dag_name = 'error_aggregates'

with models.DAG(
        dag_name,
        schedule_interval=datetime.timedelta(hours=3),
        default_args=default_args) as dag:

    error_aggregates = bigquery_etl_query(
        task_id='error_aggregates',
        destination_table='error_aggregates',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
    )
