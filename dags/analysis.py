from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.gcp import bigquery_etl_query

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 29),
    "owner": "ascholtz@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "jmccrosky@mozilla.com",
        "ascholtz@mozilla.com",
    ],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("public_analysis", default_args=default_args, schedule_interval="0 4 * * *") as dag:
    deviations = bigquery_etl_query(
        destination_table="deviations_v1",
        dataset_id="telemetry_derived",
        dag=dag,
    )
