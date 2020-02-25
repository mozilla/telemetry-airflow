import datetime

from airflow import models
from airflow.operators.subdag_operator import SubDagOperator
from utils.amplitude import export_to_amplitude

default_args = {
    'owner': 'frank@mozilla.com',
    'start_date': datetime.datetime(2019, 6, 27),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com', 'akomar@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag_name = 'bq_events_to_amplitude'

with models.DAG(
        dag_name,
        default_args=default_args,
        schedule_interval='0 1 * * *') as dag:

    fenix_task_id = 'fenix_amplitude_export'
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=fenix_task_id,
            parent_dag_name=dag_name,
            default_args=default_args,
            project='moz-fx-data-derived-datasets',
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
            project='moz-fx-data-shared-prod',
            dataset='telemetry',
            table_or_view='fennec_ios_events_v1',
            s3_prefix='fennec_ios',
        ),
        task_id=fennec_ios_task_id
    )

    focus_android_task_id = 'focus_android_amplitude_export'
    focus_args = default_args.copy()
    focus_args["start_date"] = datetime.datetime(2019, 12, 2)
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=focus_android_task_id,
            parent_dag_name=dag_name,
            default_args=focus_args,
            project='moz-fx-data-shared-prod',
            dataset='telemetry',
            table_or_view='focus_android_events_v1',
            s3_prefix='focus_android',
        ),
        task_id=focus_android_task_id
    )

    rocket_android_task_id = 'rocket_android_amplitude_export'
    rocket_args = default_args.copy()
    rocket_args["start_date"] = datetime.datetime(2019, 12, 2)
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=rocket_android_task_id,
            parent_dag_name=dag_name,
            default_args=rocket_args,
            project='moz-fx-data-shared-prod',
            dataset='telemetry',
            table_or_view='rocket_android_events_v1',
            s3_prefix='rocket_android',
        ),
        task_id=rocket_android_task_id
    )

    fire_tv_task_id = 'fire_tv_amplitude_export'
    fire_tv_args = default_args.copy()
    fire_tv_args["start_date"] = datetime.datetime(2019, 12, 2)
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=fire_tv_task_id,
            parent_dag_name=dag_name,
            default_args=fire_tv_args,
            project='moz-fx-data-shared-prod',
            dataset='telemetry',
            table_or_view='fire_tv_events_v1',
            s3_prefix='fire_tv',
        ),
        task_id=fire_tv_task_id
    )

    devtools_task_id = 'devtools_amplitude_export'
    devtools_args = default_args.copy()
    devtools_args["start_date"] = datetime.datetime(2019, 12, 2)
    devtools_args["email"] = ['ssuh@mozilla.com', 'telemetry-alerts@mozilla.com', 'akomar@mozilla.com']
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=devtools_task_id,
            parent_dag_name=dag_name,
            default_args=devtools_args,
            project='moz-fx-data-shared-prod',
            dataset='telemetry_derived',
            table_or_view='devtools_events_amplitude_v1',
            s3_prefix='devtools',
        ),
        task_id=devtools_task_id
    )
