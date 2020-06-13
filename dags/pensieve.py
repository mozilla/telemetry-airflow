from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com", "ssuh@mozilla.com", "tdsmith@mozilla.com",],
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 12),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("pensieve", default_args=default_args, schedule_interval="0 1 * * *") as dag:

    # Built from repo https://github.com/mozilla/pensieve
    pensieve_image = "gcr.io/moz-fx-data-experiments/pensieve:latest"

    pensieve = GKEPodOperator(
        task_id="pensieve",
        name="pensieve",
        image=pensieve_image,
        email=["ascholtz@mozilla.com", "ssuh@mozilla.com", "tdsmith@mozilla.com",],
        arguments=["--date={{ds}}"],
        dag=dag,
    )

    wait_for_clients_daily_export = ExternalTaskSensor(
        task_id="wait_for_clients_daily_export",
        external_dag_id="main_summary",
        external_task_id="clients_daily_export",
        dag=dag,
    )

    wait_for_main_summary_export = ExternalTaskSensor(
        task_id="wait_for_main_summary_export",
        external_dag_id="main_summary",
        external_task_id="main_summary_export",
        dag=dag,
    )

    wait_for_search_clients_daily = ExternalTaskSensor(
        task_id="wait_for_search_clients_daily",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        dag=dag,
    )

    wait_for_bq_events = ExternalTaskSensor(
        task_id="wait_for_bq_events",
        external_dag_id="main_summary",
        external_task_id="bq_main_events",
        dag=dag,
    )

    wait_for_copy_deduplicate_events = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        dag=dag,
    )

    pensieve.set_upstream(
        [
            wait_for_clients_daily_export,
            wait_for_main_summary_export,
            wait_for_search_clients_daily,
            wait_for_bq_events,
            wait_for_copy_deduplicate_events,
        ]
    )
