from airflow import models
from utils.gcp import bigquery_etl_query
from airflow.operators.sensors import ExternalTaskSensor
import datetime

default_args = {
    "owner": "bmiroglio@mozilla.com",
    "start_date": datetime.datetime(2020, 3, 16),
    "email": ["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=30),
}

dag_name = 'addons_daily'

with models.DAG(dag_name, schedule_interval='0 1 * * *', default_args=default_args) as dag:

    wait_for_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_clients_last_seen",
        external_dag_id="main_summary",
        external_task_id="clients_last_seen",
    )

    wait_for_search_clients_daily = ExternalTaskSensor(
        task_id="wait_for_search_clients_daily",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
    )

    addons_daily = bigquery_etl_query(
        task_id='addons_daily',
        destination_table='addons_daily_v1',
        project_id='moz-fx-data-shared-prod',
        dataset_id='telemetry_derived',
    )

    wait_for_clients_last_seen >> addons_daily
    wait_for_search_clients_daily >> addons_daily
