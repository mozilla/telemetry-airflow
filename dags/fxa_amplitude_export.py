import datetime

from airflow import models
from utils.gcp import bigquery_etl_query
from airflow.operators.subdag_operator import SubDagOperator
from utils.amplitude import export_to_amplitude

default_args = {
    'owner': 'frank@mozilla.com',
    'start_date': datetime.datetime(2020, 5, 1),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=20),
}

dag_name = 'fxa_amplitude_export'

with models.DAG(
        dag_name,
        schedule_interval='0 10 * * *',
        default_args=default_args) as dag:

    fxa_export_table_create = bigquery_etl_query(
        task_id='fxa_amplitude_export_v1',
        destination_table='fxa_amplitude_export_v1',
        dataset_id='firefox_accounts_derived'
    )

    task_id = 'fxa_amplitude_export_task'
    fxa_amplitude_export = SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=task_id,
            parent_dag_name=dag_name,
            default_args=default_args,
            project='moz-fx-data-shared-prod',
            dataset='firefox_accounts',
            table_or_view='fxa_amplitude_export_v1',
            s3_prefix='fxa-active',
        ),
        task_id=task_id
    )

    fxa_export_table_create >> fxa_amplitude_export
