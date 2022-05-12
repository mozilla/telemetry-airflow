"""
See [kpi-forecasting in the docker-etl repository]
(https://github.com/mozilla/docker-etl/blob/main/jobs/kpi-forecasting).

This DAG runs the forecast for year-end KPI values for Desktop QCDOU, Mobile CDOU and Pocket.
The forecast is only updated once per week since values aren't expected to change significantly
day-to-day. The output powers KPI dashboards.

This DAG is low priority.
"""

from datetime import datetime, timedelta

from airflow import DAG
from utils.gcp import gke_command
from airflow.sensors.external_task import ExternalTaskSensor
from utils.constants import ALLOWED_STATES, FAILED_STATES
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
    kpi_forecasting_mobile = gke_command(
        task_id="kpi_forecasting_mobile",
        command=[
            "python", "kpi-forecasting/kpi_forecasting.py",
            "-c",
        ] + ["kpi-forecasting/yaml/mobile.yaml"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest",
        dag=dag,
    )

    kpi_forecasting_desktop = gke_command(
        task_id="kpi_forecasting_desktop",
        command=[
            "python", "kpi-forecasting/kpi_forecasting.py",
            "-c",
        ] + ["kpi-forecasting/yaml/desktop.yaml"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest",
        dag=dag,
    )

    wait_for_mobile_usage = ExternalTaskSensor(
        task_id="wait_for_mobile_usage",
        external_dag_id="bqetl_nondesktop",
        external_task_id="telemetry_derived__mobile_usage__v1",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_unified_metrics = ExternalTaskSensor(
        task_id="wait_for_unified_metrics",
        external_dag_id="bqetl_unified",
        external_task_id="telemetry_derived__unified_metrics__v1",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_mobile_usage >> kpi_forecasting_mobile
    wait_for_unified_metrics >> kpi_forecasting_desktop
