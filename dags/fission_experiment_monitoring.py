import datetime

from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from operators.gcp_container_operator import GKEPodOperator
from utils.gcp import  bigquery_etl_query


default_args = {
    "owner": "akomar@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 1),
    "email": ["telemetry-alerts@mozilla.com", "akomar@mozilla.com", "cdowhygelund@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag_name = "fission_experiment_monitoring"

with models.DAG(
        dag_name,
        schedule_interval="0 2 * * *",
        default_args=default_args) as dag:

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    fission_monitoring_main_v1 = bigquery_etl_query(
        task_id="fission_monitoring_main_v1",
        project_id="moz-fx-data-shared-prod",
        destination_table="fission_monitoring_main_v1",
        dataset_id="telemetry_derived",
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    wait_for_copy_deduplicate_crash_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_crash_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    fission_monitoring_crash_v1 = bigquery_etl_query(
        task_id="fission_monitoring_crash_v1",
        project_id="moz-fx-data-shared-prod",
        destination_table="fission_monitoring_crash_v1",
        dataset_id="telemetry_derived",
    )

    # Built from https://github.com/mozilla/fission_monitoring_nightly
    fission_aggregation_for_dashboard = GKEPodOperator(
        task_id="fission_aggregation_for_dashboard",
        name="fission_aggregation_for_dashboard",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/fission-monitoring:latest",
        env_vars=dict(
            BQ_BILLING_PROJECT_ID="moz-fx-data-shared-prod",
            BQ_INPUT_MAIN_TABLE="moz-fx-data-shared-prod.telemetry_derived.fission_monitoring_main_v1",
            BQ_INPUT_CRASH_TABLE="moz-fx-data-shared-prod.telemetry_derived.fission_monitoring_crash_v1",
            BQ_OUTPUT_TABLE="moz-fx-data-shared-prod.analysis.fission_monitoring_analyzed_v1",
            GCS_BUCKET="fission-experiment-monitoring-dashboard",
        ),
        image_pull_policy="Always",
        dag=dag,
    )

    wait_for_copy_deduplicate_main_ping >> fission_monitoring_main_v1
    [wait_for_copy_deduplicate_main_ping, wait_for_copy_deduplicate_crash_ping] >> fission_monitoring_crash_v1
    [fission_monitoring_main_v1, fission_monitoring_crash_v1] >> fission_aggregation_for_dashboard
