from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.gcp import bigquery_etl_query

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 20),
    "owner": "chutten@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "chutten@mozilla.com",
    ],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("ssl_ratios", default_args=default_args, schedule_interval="@daily"):
    # most downstream dependency is search_clients_daily
    wait_for_main_summary = ExternalTaskSensor(
        task_id="wait_for_main_summary",
        external_dag_id="main_summary",
        external_task_id="main_summary",
        execution_delta=timedelta(hours=-1),
    )

    ssl_ratios = bigquery_etl_query(
        destination_table="ssl_ratios_v1",
        dataset_id="telemetry_derived",
    )

    ssl_ratios.set_upstream(wait_for_main_summary)
