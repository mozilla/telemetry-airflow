from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor

from utils.gcp import gke_command

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

project_id = "mau-dashboards-test-1"

with DAG(
        "mau_dashboard_test",
        default_args=default_args,
        schedule_interval="0 5 * * *",
) as dag:

    wait_for_desktop_forecasts = ExternalTaskSensor(
        task_id="wait_for_desktop_forecasts",
        external_dag_id="kpi_forecasts",
        external_task_id="simpleprophet_forecasts_desktop",
        check_existence=True,
        mode="reschedule",
        execution_delta=timedelta(hours=1),
        dag=dag,
    )

    wait_for_mobile_forecasts = ExternalTaskSensor(
        task_id="wait_for_mobile_forecasts",
        external_dag_id="kpi_forecasts",
        external_task_id="simpleprophet_forecasts_mobile",
        check_existence=True,
        mode="reschedule",
        execution_delta=timedelta(hours=1),
        dag=dag,
    )

    desktop_mau = gke_command(
        task_id="desktop_mau",
        command=[
            "python", "desktop_mau/desktop_mau_dau.py",
            "--project", project_id,
            "--bucket-name=mau-dashboard",
        ],
        docker_image=f"gcr.io/{project_id}/desktop-mobile-mau-2020_docker_etl:latest",
        gcp_conn_id="google_cloud_derived_datasets",
        dag=dag,
        email=["bewu@mozilla.com"],
    )

    mobile_mau = gke_command(
        task_id="mobile_mau",
        command=[
            "python", "mobile_mau/mobile_mau.py",
            "--project", project_id,
            "--bucket-name=mau-dashboard",
        ],
        docker_image=f"gcr.io/{project_id}/desktop-mobile-mau-2020_docker_etl:latest",
        gcp_conn_id="google_cloud_derived_datasets",
        dag=dag,
        email=["bewu@mozilla.com"],
    )

    desktop_mau.set_upstream(wait_for_desktop_forecasts)

    mobile_mau.set_upstream(wait_for_mobile_forecasts)
