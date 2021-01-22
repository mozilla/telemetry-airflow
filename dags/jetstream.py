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

with DAG("jetstream", default_args=default_args, schedule_interval="0 4 * * *") as dag:

    # Built from repo https://github.com/mozilla/jetstream
    jetstream_image = "gcr.io/moz-fx-data-experiments/jetstream:latest"

    jetstream = GKEPodOperator(
        task_id="jetstream",
        name="jetstream",
        image=jetstream_image,
        email=["ascholtz@mozilla.com", "ssuh@mozilla.com", "tdsmith@mozilla.com",],
        arguments=[
            "run-argo", 
            "--date={{ ds }}",
            # the Airflow cluster doesn't have Compute Engine API access so pass in IP 
            # and certificate in order for the pod to connect to the Kubernetes cluster
            # running Jetstream 
            "--cluster-ip={{ var.value.jetstream_cluster_ip }}",
            "--cluster-cert={{ var.value.jetstream_cluster_cert }}"],
        dag=dag,
    )

    wait_for_clients_daily_export = ExternalTaskSensor(
        task_id="wait_for_clients_daily",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    wait_for_main_summary_export = ExternalTaskSensor(
        task_id="wait_for_main_summary",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_summary__v4",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    wait_for_search_clients_daily = ExternalTaskSensor(
        task_id="wait_for_search_clients_daily",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    wait_for_bq_events = ExternalTaskSensor(
        task_id="wait_for_bq_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    wait_for_copy_deduplicate_events = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    jetstream.set_upstream(
        [
            wait_for_clients_daily_export,
            wait_for_main_summary_export,
            wait_for_search_clients_daily,
            wait_for_bq_events,
            wait_for_copy_deduplicate_events,
        ]
    )
