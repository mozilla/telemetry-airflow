"""
See [kpi-forecasting in the docker-etl repository]
(https://github.com/mozilla/docker-etl/blob/main/jobs/kpi-forecasting).

This DAG runs the forecast for year-end KPI values for Desktop QDAU and Mobile DAU. These forecasts can then be aggregated to calculate Desktop QCDOU and Mobile CDOU.
The forecast is only updated once per week since values aren't expected to change significantly day-to-day. 
The output powers KPI dashboards and monthly revenue forecasts.

This DAG is high priority for week 1 of the month. This DAG is low priority for weeks 2-4 of each month. 
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

tags = [Tag.ImpactTier.tier_1]

with DAG("kpi_forecasting", default_args=default_args, schedule_interval="0 4 * * SAT", doc_md=__doc__, tags=tags,) as dag:
    
    kpi_forecasting_desktop_non_cumulative = gke_command(
        task_id="kpi_forecasting_desktop_non_cumulative",
        command=[
            "python", "kpi-forecasting/kpi_forecasting.py",
            "-c",
        ] + ["kpi-forecasting/yaml/desktop_non_cumulative.yaml"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest",
        dag=dag,
    )

    kpi_forecasting_mobile_non_cumulative = gke_command(
        task_id="kpi_forecasting_mobile_non_cumulative",
        command=[
            "python", "kpi-forecasting/kpi_forecasting.py",
            "-c",
        ] + ["kpi-forecasting/yaml/mobile_non_cumulative.yaml"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest",
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

    wait_for_unified_metrics >> kpi_forecasting_desktop_non_cumulative
    wait_for_unified_metrics >> kpi_forecasting_mobile_non_cumulative
