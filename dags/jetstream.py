"""
Powers the [jetstream](https://experimenter.info/jetstream/jetstream/)
analysis framework for experiments.

See the [jetstream repository](https://github.com/mozilla/jetstream).
"""

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com", "kignasiak@mozilla.com",],
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 12),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG(
        "jetstream",
        default_args=default_args,
        schedule_interval="0 4 * * *",
        doc_md=__doc__,
        tags=tags,
) as dag:

    # Built from repo https://github.com/mozilla/jetstream
    jetstream_image = "gcr.io/moz-fx-data-experiments/jetstream:latest"

    jetstream_run = GKEPodOperator(
        task_id="jetstream_run",
        name="jetstream_run",
        image=jetstream_image,
        email=["ascholtz@mozilla.com", "kignasiak@mozilla.com",],
        arguments=[
            "--log_to_bigquery",
            "run-argo", 
            "--date={{ ds }}",
            # the Airflow cluster doesn't have Compute Engine API access so pass in IP 
            # and certificate in order for the pod to connect to the Kubernetes cluster
            # running Jetstream 
            "--cluster-ip={{ var.value.jetstream_cluster_ip }}",
            "--cluster-cert={{ var.value.jetstream_cluster_cert }}"],
        dag=dag,
    )

    jetstream_config_changed = GKEPodOperator(
        task_id="jetstream_run_config_changed",
        name="jetstream_run_config_changed",
        image=jetstream_image,
        email=["ascholtz@mozilla.com", "kignasiak@mozilla.com",],
        arguments=[
            "--log_to_bigquery",
            "rerun-config-changed",
            "--argo",
            # the Airflow cluster doesn't have Compute Engine API access so pass in IP 
            # and certificate in order for the pod to connect to the Kubernetes cluster
            # running Jetstream
            "--cluster-ip={{ var.value.jetstream_cluster_ip }}",
            "--cluster-cert={{ var.value.jetstream_cluster_cert }}"],
        dag=dag,
    )

    wait_for_clients_daily_export = ExternalTaskCompletedSensor(
        task_id="wait_for_clients_daily",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_main_summary_export = ExternalTaskCompletedSensor(
        task_id="wait_for_main_summary",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_summary__v4",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_search_clients_daily = ExternalTaskCompletedSensor(
        task_id="wait_for_search_clients_daily",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_bq_events = ExternalTaskCompletedSensor(
        task_id="wait_for_bq_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_events = ExternalTaskCompletedSensor(
        task_id="wait_for_copy_deduplicate_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    jetstream_run.set_upstream(
        [
            wait_for_clients_daily_export,
            wait_for_main_summary_export,
            wait_for_search_clients_daily,
            wait_for_bq_events,
            wait_for_copy_deduplicate_events,
        ]
    )
    jetstream_config_changed.set_upstream(jetstream_run)
