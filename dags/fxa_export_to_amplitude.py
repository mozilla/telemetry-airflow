import datetime
import pendulum

from airflow import models
from utils.gcp import bigquery_etl_query
from airflow.operators.subdag_operator import SubDagOperator
from utils.amplitude import export_to_amplitude

# https://airflow.apache.org/docs/stable/timezone.html#time-zone-aware-dags
pt_tz = pendulum.timezone("America/Los_Angeles")

default_args = {
    'owner': 'frank@mozilla.com',
    'start_date': datetime.datetime(2020, 4, 1, tzinfo=pt_tz),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=20),
}

dag_name = 'fxa_export_to_amplitude'

"""
A Note on Times:
FxA logs become available to BigQuery within seconds.
The `timestamp` field of an event is when it occurred on the server,
and the `receiveTimestamp` was when it was received by Cloud Logging.
Usually these are at most seconds apart.
Reference: https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry

From there the data is streamed to BigQuery. This is one record at-a-time
and data should be available very quickly.
Reference: https://cloud.google.com/logging/docs/export/bigquery

However, the above exports to UTC-partitioned tables. We are running
with PDT-based days, so the following will actually wait until
7:30:00 hours after start of day UTC (for PDT) and 8:30:00 after (during PST).
https://airflow.apache.org/docs/stable/timezone.html#cron-schedules
"""
with models.DAG(
        dag_name,
        schedule_interval='30 0 * * *',
        catchup=True,
        default_args=default_args) as dag:

    fxa_export_table_create = bigquery_etl_query(
        task_id='fxa_amplitude_export_v1',
        project_id='moz-fx-data-shared-prod',
        destination_table='fxa_amplitude_export_v1',
        dataset_id='firefox_accounts_derived',
        depends_on_past=True
    )

    task_id = 'fxa_amplitude_export_task'
    fxa_amplitude_export = SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=task_id,
            parent_dag_name=dag_name,
            default_args=default_args,
            project='moz-fx-data-shared-prod',
            dataset='firefox_accounts',
            table_or_view='fxa_amplitude_export',
            s3_prefix='fxa-active',
        ),
        task_id=task_id
    )

    fxa_export_table_create >> fxa_amplitude_export

    fxa_amplitude_user_ids = bigquery_etl_query(
        task_id='fxa_amplitude_user_ids',
        project_id='moz-fx-data-shared-prod',
        destination_table='fxa_amplitude_user_ids_v1',
        dataset_id='firefox_accounts_derived',
        depends_on_past=True,
        start_date=datetime.datetime(2020, 5, 10),
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
        # This query updates the entire existing table every day rather than appending
        # a new partition, so we need to disable date_partition_parameter and instead
        # pass submission_date as a generic param.
        date_partition_parameter=None,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    sync_send_tab_task_id = 'sync_send_tab_amplitude_export'
    sync_send_tab_args = default_args.copy()
    sync_send_tab_args['email'] = ['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com']
    sync_send_tab_args['owner'] = 'jklukas@mozilla.com'
    sync_send_tab_export = SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=sync_send_tab_task_id,
            parent_dag_name=dag_name,
            default_args=sync_send_tab_args,
            project='moz-fx-data-shared-prod',
            dataset='firefox_accounts',
            table_or_view='sync_send_tab_export',
            s3_prefix='sync_send_tab',
        ),
        task_id=sync_send_tab_task_id
    )

    fxa_amplitude_user_ids >> sync_send_tab_export
