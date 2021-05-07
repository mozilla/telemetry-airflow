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
        schedule_interval="0 3 * * *",
        default_args=default_args) as dag:

    wait_for_main_nightly = ExternalTaskSensor(
        task_id="wait_for_main_nightly",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_nightly__v1",
        execution_delta=datetime.timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
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

    # Built from https://github.com/mozilla/fission_monitoring_nightly
    fission_aggregation_for_dashboard = GKEPodOperator(
        task_id="fission_aggregation_for_dashboard",
        name="fission_aggregation_for_dashboard",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/fission-monitoring:latest",
        env_vars=dict(
            BQ_BILLING_PROJECT_ID="mozdata",
            BQ_INPUT_MAIN_TABLE="mozdata.telemetry.main_nightly",
            BQ_INPUT_CRASH_TABLE="mozdata.telemetry.crash",
            BQ_TMP_CRASH_TABLE="mozdata.tmp.fission_nightly_exp_crash",
            BQ_OUTPUT_TABLE="mozdata.analysis.fission_monitoring_analyzed_v1",
            GCS_BUCKET="fission-experiment-monitoring-dashboard",
        ),
        image_pull_policy="Always",
        dag=dag,
    )

    [wait_for_main_nightly, wait_for_copy_deduplicate_crash_ping] >> fission_aggregation_for_dashboard
