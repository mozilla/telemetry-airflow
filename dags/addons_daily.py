from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar

default_args = {
    "owner": "bmiroglio@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2019, 6, 10),
    "email": [
        "telemetry-alerts@mozilla.com",
        "bmiroglio@mozilla.com",
        "smelancon@mozilla.com",
        "bwright@mozilla.com",
        "dthorn@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("addons_daily", default_args=default_args, schedule_interval="@daily")

# most downstream dependency is search_clients_daily
wait_for_search_clients_daily = ExternalTaskSensor(
    task_id="wait_for_search_clients_daily",
    external_dag_id="main_summary",
    external_task_id="search_clients_daily",
    execution_delta=timedelta(hours=-1),
    dag=dag,
)

addons_daily = MozDatabricksSubmitRunOperator(
    task_id="addons_daily",
    job_name="Addons Daily",
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    owner="bmiroglio@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "bmiroglio@mozilla.com",
        "smelancon@mozilla.com",
        "bwright@mozilla.com",
        "dthorn@mozilla.com",
    ],
    env=mozetl_envvar(
        "addons_daily",
        {
            "date": "{{ ds_nodash }}",
            "deploy_environment": "{{ task.__class__.deploy_environment }}",
        },
        other={
            "MOZETL_GIT_PATH": "https://github.com/mozilla/addons_daily.git",
            "MOZETL_EXTERNAL_MODULE": "addons_daily.addons_report",
        },
    ),
    dag=dag,
)

addons_daily.set_upstream(wait_for_search_clients_daily)
