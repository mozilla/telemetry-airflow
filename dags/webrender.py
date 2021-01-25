from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "email": ["amiyaguchi@mozilla.com", "tdsmith@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 19),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    "webrender_ds_283", default_args=default_args, schedule_interval="0 2 * * *"
) as dag:

    # Make sure all the data for the given day has arrived before running.
    wait_for_main_ping = ExternalTaskSensor(
        task_id="wait_for_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    # Built from repo https://github.com/mozilla/webrender_intel_win10_nightly
    webrender_ds_283 = GKEPodOperator(
        task_id="webrender_ds_283",
        name="webrender_ds_283",
        image="gcr.io/moz-fx-ds-283/ds_283_prod:latest",
        env_vars=dict(
            BUCKET="gs://moz-fx-ds-283",
            PROJECT_ID="moz-fx-data-shared-prod",
            # source dataset, results are written to the analysis dataset
            DATASET="telemetry",
        ),
        dag=dag,
    )

    wait_for_main_ping >> webrender_ds_283
