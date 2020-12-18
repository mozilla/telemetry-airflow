from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.gcp import bigquery_etl_query
from operators.gcp_container_operator import GKEPodOperator
from airflow.models import Variable

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 1),
    "owner": "ascholtz@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "aplacitelli@mozilla.com",
        "ascholtz@mozilla.com",
    ],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("anomdtct", default_args=default_args, schedule_interval="0 3 * * *") as dag:
    # Built from https://github.com/mozilla/forecasting/tree/master/anomdtct
    anomdtct_image = "gcr.io/moz-fx-data-airflow-prod-88e0/anomdtct:latest"

    anomdtct = GKEPodOperator(
        task_id="anomdtct",
        name="anomdtct",
        image=anomdtct_image,
        email=["ascholtz@mozilla.com", "aplacitelli@mozilla.com",],
        arguments=["{{ds}}"],
        dag=dag,
    )

    wait_for_clients_first_seen = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_first_seen__v1",
        execution_delta=timedelta(hours=1),
        dag=dag,
    )

    anomdtct.set_upstream(
        [
            wait_for_clients_first_seen,
        ]
    )
