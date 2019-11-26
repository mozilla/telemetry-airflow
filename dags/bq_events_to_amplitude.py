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

    fenix_task_id = 'fenix_amplitude_export'
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=fenix_task_id,
            parent_dag_name=dag_name,
            default_args=default_args,
            dataset='telemetry',
            table_or_view='fenix_events_v1',
            s3_prefix='fenix',
        ),
        task_id=fenix_task_id
    )

    fennec_ios_task_id = 'fennec_ios_amplitude_export'
    fennec_ios_args = default_args.copy()
    fennec_ios_args["start_date"] = datetime.datetime(2019, 12, 2)
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=fennec_ios_task_id,
            parent_dag_name=dag_name,
            default_args=fennec_ios_args,
            dataset='telemetry',
            table_or_view='fennec_ios_events_v1',
            s3_prefix='fennec_ios',
        ),
        task_id=fennec_ios_task_id
    )
