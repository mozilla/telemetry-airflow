"""
See [kpi-forecasting in the docker-etl repository](https://github.com/mozilla/docker-etl/blob/main/jobs/kpi-forecasting).

This DAG runs the search forecasts for the DAU, search count and ad clicks metrics .

This DAG is high priority for week 1 of the month and low priority otherwise.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "jsnyder@mozilla.com",
    "email": [
        "jsnyder@mozilla.com",
        "mbowerman@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 6),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

TAGS = [Tag.ImpactTier.tier_1]
IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest"

FORECAST_METRICS_LIST = [
    "search_forecasting_daily_active_users",
    "search_forecasting_search_count",
    "search_forecasting_ad_clicks",
]

with DAG(
    "search_forecasting",
    default_args=default_args,
    schedule_interval="7 7 7 * *",
    doc_md=__doc__,
    tags=TAGS,
) as dag:
    # all the search forecasting metrics come from the search_revenue_levers_daily
    # table which is run in the bqetl_search_dashboard dag
    # as the search_derived__search_revenue_levers_daily__v1 task
    # see: https://workflow.telemetry.mozilla.org/dags/bqetl_search_dashboard/grid
    wait_task_sensor = ExternalTaskSensor(
        task_id="wait_for_search_dashboard",
        external_dag_id="bqetl_search_dashboard",
        external_task_id="search_derived__search_revenue_levers_daily__v1",
        execution_delta=timedelta(days=7),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    for metric in FORECAST_METRICS_LIST:
        # pass the search_forecasting configs to the KPI forecasting script
        config_filename = f"{metric}.yaml"
        script_path = os.path.join(".", "kpi_forecasting.py")
        config_path = os.path.join("kpi_forecasting", "configs", config_filename)

        forecast_task = GKEPodOperator(
            task_id=f"search_forecasting_{metric}",
            arguments=["python", script_path, "-c", config_path],
            image=IMAGE,
        )

        wait_task_sensor >> forecast_task
