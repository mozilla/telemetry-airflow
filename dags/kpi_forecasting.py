"""
See [kpi-forecasting in the docker-etl repository](https://github.com/mozilla/docker-etl/blob/main/jobs/kpi-forecasting).

This DAG runs the forecast Desktop DAU and Mobile DAU. The output powers KPI dashboards and monthly revenue forecasts.

This DAG is high priority for week 1 of the month and low priority otherwise.
"""
import os
from collections import namedtuple
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import gke_command
from utils.tags import Tag

default_args = {
    "owner": "bochocki@mozilla.com",
    "email": ["bochocki@mozilla.com", "jsilverman@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 28),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

TAGS = [Tag.ImpactTier.tier_1]
IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest"

Config = namedtuple("Config", ["filename", "wait_dag", "wait_tasks"])
CONFIGS = {
    "dau_desktop": Config(
        "dau_desktop.yaml",
        "dim_active_users_aggregates_desktop",
        [
            "firefox_desktop_active_users_aggregates_dim_check",
        ],
    ),
    "dau_mobile": Config(
        "dau_mobile.yaml",
        "dim_active_users_aggregates_mobile",
        [
            "fenix_active_users_aggregates_dim_check",
            "firefox_ios_active_users_aggregates_dim_check",
            "focus_android_active_users_aggregates_dim_check",
            "focus_ios_active_users_aggregates_dim_check",
        ],
    ),
}

with DAG(
    "kpi_forecasting",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=__doc__,
    tags=TAGS,
) as dag:
    for id, config in CONFIGS.items():
        script_path = os.path.join("kpi-forecasting", "kpi_forecasting.py")
        config_path = os.path.join("kpi-forecasting", "configs", config.filename)
        wait_tasks = config.wait_tasks

        if not isinstance(config.wait_tasks, list):
            wait_tasks = [wait_tasks]

        forecast_task = gke_command(
            task_id=f"kpi_forecasting_{id}",
            command=["python", script_path, "-c", config_path],
            docker_image=IMAGE,
            dag=dag,
        )

        for wait_task in wait_tasks:
            wait_task_sensor = ExternalTaskSensor(
                task_id=f"wait_for_{wait_task}",
                external_dag_id=config.wait_dag,
                external_task_id=wait_task,
                execution_delta=timedelta(minutes=30),
                check_existence=True,
                mode="reschedule",
                allowed_states=ALLOWED_STATES,
                failed_states=FAILED_STATES,
                pool="DATA_ENG_EXTERNALTASKSENSOR",
            )

            wait_task_sensor >> forecast_task
