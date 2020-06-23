import datetime

from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from utils.amplitude import export_to_amplitude
from utils.gcp import gke_command

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
            project='moz-fx-data-shared-prod',
            dataset='telemetry',
            table_or_view='fenix_events_v1',
            s3_prefix='fenix',
        ),
        task_id=fenix_task_id
    )

    fenix_shredder = gke_command(
        task_id="fenix_amplitude_shredder",
        name="shredder-amplitude-fenix",
        command=[
            "script/shredder_amplitude",
            "--date={{ ds }}",
            "--api-key={{ var.value.amplitude_api_key }}",
            "--secret-key={{ var.value.amplitude_secret_key }}",
            "--table-id=moz-fx-data-shared-prod.org_mozilla_fenix.deletion_request_v1",
            "--device-id-field=client_info.client_id",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        dag=dag,
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

    # DevTools view merges events from `telemetry.main` and `telemetry.event`.
    # We need to make sure both tables are ready and deduplicated before proceeding.
    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        dag=dag)
    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="main_summary",
        external_task_id="copy_deduplicate_main_ping",
        dag=dag,
    )

    devtools_task_id = 'devtools_amplitude_export'
    devtools_args = default_args.copy()
    devtools_args["start_date"] = datetime.datetime(2019, 12, 2)
    devtools_args["email"] = ['ssuh@mozilla.com', 'telemetry-alerts@mozilla.com', 'akomar@mozilla.com']
    devtools_export = SubDagOperator(
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

    devtools_shredder = gke_command(
        task_id="devtools_amplitude_shredder",
        name="shredder-amplitude-devtools",
        command=[
            "script/shredder_amplitude",
            "--date={{ ds }}",
            "--api-key={{ var.value.amplitude_api_key }}",
            "--secret-key={{ var.value.amplitude_secret_key }}",
            "--table-id=moz-fx-data-shared-prod.telemetry.deletion_request_v4",
            "--user-id-field=client_id",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        dag=dag,
    )

    [wait_for_copy_deduplicate_all, wait_for_copy_deduplicate_main_ping] >> devtools_export
    wait_for_copy_deduplicate_all >> [fenix_shredder, devtools_shredder]

    onboarding_task_id = 'onboarding_amplitude_export'
    onboarding_args = default_args.copy()
    onboarding_args["start_date"] = datetime.datetime(2020, 6, 25)
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=onboarding_task_id,
            parent_dag_name=dag_name,
            default_args=onboarding_args,
            project='moz-fx-data-shared-prod',
            dataset='messaging_system',
            table_or_view='onboarding_events_amplitude',
            s3_prefix='onboarding',
        ),
        task_id=onboarding_task_id
    )
