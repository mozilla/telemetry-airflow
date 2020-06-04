from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.gcp import bigquery_etl_query
from operators.gcp_container_operator import GKEPodOperator
from airflow.models import Variable

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

with DAG("public_analysis", default_args=default_args, schedule_interval="0 1 * * *") as dag:
    # Built from https://github.com/mozilla/forecasting/tree/master/anomdtct
    anomdtct_image = "gcr.io/moz-fx-data-forecasting/anomdtct:latest"

    anomdtct = GKEPodOperator(
        task_id="anomdtct",
        name="anomdtct",
        image=anomdtct_image,
        email=["ascholtz@mozilla.com", "jmccrosky@mozilla.com",],
        arguments=["{{ds}}"]
        + ["--spreadsheet-id=" + Variable.get('anomdtct_spreadsheet_id')]
        + ["--spreadsheet-key=" + Variable.get('anomdtct_spreadsheet_api_key')],
        dag=dag,
    )

    wait_for_clients_first_seen = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen",
        external_dag_id="main_summary",
        external_task_id="clients_first_seen",
        dag=dag,
    )

    anomdtct.set_upstream(
        [
            wait_for_clients_first_seen,
        ]
    )

    deviations = bigquery_etl_query(
        task_id="deviations",
        project_id="moz-fx-data-shared-prod",
        destination_table="deviations_v1",
        dataset_id="telemetry_derived",
        arguments=("--replace",),
        dag=dag,
    )

    deviations.set_upstream(anomdtct)
