import datetime

from airflow import models
from airflow.operators.subdag_operator import SubDagOperator
from utils.amplitude import export_to_amplitude

default_args = {
    'owner': 'frank@mozilla.com',
    'start_date': datetime.datetime(2019, 6, 27),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=10),
    'schedule_interval': '0 1 * * *',
}

dag_name = 'bq_events_to_amplitude'

with models.DAG(
        dag_name,
        default_args=default_args) as dag:

    task_id = 'fenix_amplitude_export'
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=task_id,
            parent_dag_name=dag_name,
            default_args=default_args,
            dataset='telemetry',
            table_or_view='fenix_events_v1',
            s3_prefix='fenix',
            amplitude_s3_bucket=amplitude_s3_bucket,
        ),
        task_id=task_id
    )
