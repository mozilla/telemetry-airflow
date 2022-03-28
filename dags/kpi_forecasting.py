"""
See [kpi-forecasting in the docker-etl repository]
(https://github.com/mozilla/docker-etl/blob/main/jobs/kpi-forecasting).

This DAG is low priority.
"""

from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import gke_command
from operators.task_sensor import ExternalTaskCompletedSensor
from utils.tags import Tag

default_args = {
    "owner": "pmcmanis@mozilla.com",
    "email": ["pmcmanis@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 28),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

with DAG("kpi_forecasting", default_args=default_args, schedule_interval="0 4 * * SAT", doc_md=__doc__, tags=tags,) as dag:
    dataset_yamls = ["yaml/mobile.yaml", "yaml/desktop.yaml"]

    kpi_forecasting = gke_command(
        task_id="kpi_forecasting",
        command=[
            "python", "kpi-forecasting/kpi-forecasting.py",
            "-c",
        ] + dataset_yamls,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest",
        dag=dag,
    )

    wait_for_clients_daily = ExternalTaskCompletedSensor(
        task_id="wait_for_clients_daily",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag
    )

    wait_for_main_ping = ExternalTaskCompletedSensor(
        task_id="wait_for_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=timedelta(hours=2),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_mobile_usage = ExternalTaskCompletedSensor(
        task_id="wait_for_mobile_usage",
        external_dag_id="bqetl_nondesktop",
        external_task_id="telemetry_derived__mobile_usage__v1",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_unified_metrics = ExternalTaskCompletedSensor(
        task_id="wait_for_unified_metrics",
        external_dag_id="bqetl_unified",
        external_task_id="telemetry_derived__unified_metrics__v1",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    [
        wait_for_clients_daily, 
        wait_for_main_ping, 
        wait_for_mobile_usage, 
        wait_for_unified_metrics
    ] >> kpi_forecasting
