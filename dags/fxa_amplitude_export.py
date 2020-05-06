import datetime

from airflow import models
from utils.gcp import bigquery_etl_query
from airflow.operators.subdag_operator import SubDagOperator
from utils.amplitude import export_to_amplitude

default_args = {
    'owner': 'frank@mozilla.com',
    'start_date': datetime.datetime(2020, 4, 1),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=20),
}

dag_name = 'fxa_amplitude_export'

"""
FxA logs become available to BigQuery within seconds.
The `timestamp` field of an event is when it occurred on the server,
and the `receiveTimestamp` was when it was received by Cloud Logging.
Usually these are at most seconds apart.
Reference: https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry

From there the data is streamed to BigQuery. This is one record at-a-time
and data should be available very quickly.
Reference: https://cloud.google.com/logging/docs/export/bigquery
"""
with models.DAG(
        dag_name,
        schedule_interval='30 0 * * *',
        default_args=default_args) as dag:

    fxa_export_table_create = bigquery_etl_query(
        task_id='fxa_amplitude_export_v1',
        project_id='moz-fx-data-shared-prod',
        destination_table='fxa_amplitude_export_v1',
        dataset_id='firefox_accounts_derived'
    )

    task_id = 'fxa_amplitude_export_task'
    fxa_amplitude_export = SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=task_id,
            parent_dag_name=dag_name,
            default_args=dict(default_args, **{'depends_on_past': True}),
            project='moz-fx-data-shared-prod',
            dataset='firefox_accounts',
            table_or_view='fxa_amplitude_export',
            s3_prefix='fxa-active',
        ),
        task_id=task_id
    )

    fxa_export_table_create >> fxa_amplitude_export
