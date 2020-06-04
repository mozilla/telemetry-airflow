from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime
from utils.gcp import gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 14),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("public_data", default_args=default_args, schedule_interval="0 1 * * *") as dag:
    docker_image = "mozilla/bigquery-etl:latest"

    public_data_gcs_metadata = gke_command(
        task_id="public_data_gcs_metadata",
        command=["script/publish_public_data_gcs_metadata"],
        docker_image=docker_image,
        dag=dag
    )

    wait_for_deviations = ExternalTaskSensor(
        task_id="wait_for_deviations",
        external_dag_id="bqetl_deviations",
        external_task_id="telemetry_derived__deviations__v1",
        dag=dag,
    )

    wait_for_ssl_ratios = ExternalTaskSensor(
        task_id="wait_for_ssl_ratios",
        external_dag_id="ssl_ratios",
        external_task_id="ssl_ratios",
        execution_delta=timedelta(hours=1),
        dag=dag,
    )

    public_data_gcs_metadata.set_upstream(
        [
            wait_for_deviations,
            wait_for_ssl_ratios,
        ]
    )
