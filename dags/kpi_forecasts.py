import datetime

from airflow import models
from datetime import timedelta
from utils.gcp import bigquery_etl_query
from airflow.operators.sensors import ExternalTaskSensor
from utils.forecasting import simpleprophet_forecast

default_args = {
    'owner': 'jklukas@mozilla.com',
    'start_date': datetime.datetime(2019, 5, 12),
    'email': ['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag_name = 'kpi_forecasts'

with models.DAG(
        dag_name,
        schedule_interval='0 4 * * *',
        default_args=default_args) as dag:

    simpleprophet_forecasts_mobile = simpleprophet_forecast(
        task_id="simpleprophet_forecasts_mobile",
        datasource="mobile",
        project_id='moz-fx-data-shared-prod',
        dataset_id='telemetry_derived',
        table_id='simpleprophet_forecasts_mobile_v1',
        owner="jklukas@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    )

    wait_for_firefox_nondesktop_exact_mau28 = ExternalTaskSensor(
        task_id="wait_for_firefox_nondesktop_exact_mau28",
        external_dag_id="bqetl_nondesktop",
        external_task_id="telemetry_derived__firefox_nondesktop_exact_mau28__v1",
        check_existence=True,
        mode="reschedule",
        execution_delta=timedelta(hours=1),
        dag=dag,
    )

    simpleprophet_forecasts_mobile.set_upstream(wait_for_firefox_nondesktop_exact_mau28)

    simpleprophet_forecasts_desktop = simpleprophet_forecast(
        task_id="simpleprophet_forecasts_desktop",
        datasource="desktop",
        project_id='moz-fx-data-shared-prod',
        dataset_id='telemetry_derived',
        table_id='simpleprophet_forecasts_desktop_v1',
        owner="jklukas@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
        dag=dag)

    wait_for_exact_mau_by_dimensions = ExternalTaskSensor(
        task_id="wait_for_exact_mau_by_dimensions",
        external_dag_id="bqetl_main_summary",
        external_task_id="firefox_desktop_exact_mau28_by_dimensions",
        check_existence=True,
        mode="reschedule",
        execution_delta=timedelta(hours=2),
        dag=dag,
    )

    simpleprophet_forecasts_desktop.set_upstream(wait_for_exact_mau_by_dimensions)

    simpleprophet_forecasts_fxa = simpleprophet_forecast(
        task_id="fxa_simpleprophet_forecasts",
        datasource="fxa",
        project_id='moz-fx-data-shared-prod',
        dataset_id='telemetry_derived',
        table_id='simpleprophet_forecasts_fxa_v1',
        email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    )

    wait_for_firefox_accounts_exact_mau = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_exact_mau",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__exact_mau28__v1",
        check_existence=True,
        mode="reschedule",
        execution_delta=timedelta(hours=2, minutes=30),
        dag=dag,
    )

    simpleprophet_forecasts_fxa.set_upstream(wait_for_firefox_accounts_exact_mau)
