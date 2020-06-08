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

dag_name = 'kpi_dashboard'

with models.DAG(
        dag_name,
        # KPI dashboard refreshes at 16:00 UTC, so run this 15 minutes beforehand.
        schedule_interval='45 15 * * *',
        default_args=default_args) as dag:

    kpi_dashboard = bigquery_etl_query(
        destination_table='firefox_kpi_dashboard_v1',
        dataset_id='telemetry',
        date_partition_parameter=None,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com']
    )

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
        external_task_id="telemetry__firefox_nondesktop_exact_mau28_raw__v1",
        check_existence=True,
        execution_delta=timedelta(hours=14, minutes=45),
        dag=dag,
    )

    wait_for_firefox_nondesktop_exact_mau28 >> simpleprophet_forecasts_mobile
