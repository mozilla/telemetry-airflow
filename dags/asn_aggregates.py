from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import timedelta, datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com", "tdsmith@mozilla.com",],
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 5),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("asn_aggregates", default_args=default_args, schedule_interval="0 3 * * *") as dag:

    asn_aggregates = bigquery_etl_query(
        task_id="asn_aggregates",
        destination_table="asn_aggregates_v1",
        project_id="moz-fx-data-shared-prod",
        dataset_id="telemetry_derived",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "tdsmith@mozilla.com"],
        parameters=("n_clients:INT64:500",),
        dag=dag)

    wait_for_bq_events = ExternalTaskSensor(
        task_id="wait_for_bq_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=timedelta(hours=2),
        dag=dag,
    )

    wait_for_copy_deduplicate_events = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=timedelta(hours=2),
        dag=dag,
    )

    asn_aggregates.set_upstream(
        [
            wait_for_bq_events,
            wait_for_copy_deduplicate_events,
        ]
    )
