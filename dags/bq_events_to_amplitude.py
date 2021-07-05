import datetime

from airflow import models
from operators.task_sensor import ExternalTaskCompletedSensor
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
        schedule_interval='0 2 * * *') as dag:

    shredder_fenix = gke_command(
        task_id="shredder_amplitude_fenix",
        name="shredder-amplitude-fenix",
        command=[
            "script/shredder_amplitude",
            "--date={{ ds }}",
            "--api-key={{ var.value.fenix_amplitude_api_key }}",
            "--secret-key={{ var.value.fenix_amplitude_secret_key }}",
            "--table-id=moz-fx-data-shared-prod.org_mozilla_fenix_stable.deletion_request_v1",
            "--device-id-field=client_info.client_id",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        dag=dag,
    )

    # DevTools view merges events from `telemetry.main` and `telemetry.event`.
    # We need to make sure both tables are ready and deduplicated before proceeding.
    wait_for_copy_deduplicate_all = ExternalTaskCompletedSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag)
    wait_for_copy_deduplicate_main_ping = ExternalTaskCompletedSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(hours=1),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    devtools_task_id = 'devtools_amplitude_export'
    devtools_args = default_args.copy()
    devtools_args["start_date"] = datetime.datetime(2019, 12, 2)
    devtools_args["email"] = ['telemetry-alerts@mozilla.com', 'akomar@mozilla.com']
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

    shredder_devtools = gke_command(
        task_id="shredder_amplitude_devtools",
        name="shredder-amplitude-devtools",
        command=[
            "script/shredder_amplitude",
            "--date={{ ds }}",
            "--api-key={{ var.value.devtools_amplitude_api_key }}",
            "--secret-key={{ var.value.devtools_amplitude_secret_key }}",
            "--table-id=moz-fx-data-shared-prod.telemetry_stable.deletion_request_v4",
            "--user-id-field=client_id",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        dag=dag,
    )

    [wait_for_copy_deduplicate_all, wait_for_copy_deduplicate_main_ping] >> devtools_export
    wait_for_copy_deduplicate_all >> [shredder_fenix, shredder_devtools]

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

    onboarding_retention_task_id = 'onboarding_retention_amplitude_export'
    onboarding_retention_args = default_args.copy()
    onboarding_retention_args["start_date"] = datetime.datetime(2020, 8, 30)
    SubDagOperator(
        subdag=export_to_amplitude(
            dag_name=onboarding_retention_task_id,
            parent_dag_name=dag_name,
            default_args=onboarding_retention_args,
            project='moz-fx-data-shared-prod',
            dataset='messaging_system',
            table_or_view='onboarding_retention_events_amplitude',
            s3_prefix='onboarding_retention',
        ),
        task_id=onboarding_retention_task_id
    )

    shredder_amplitude_fxa = gke_command(
        task_id='shredder_amplitude_fxa',
        name='shredder-amplitude-fxa',
        command=[
            'script/shredder_amplitude',
            '--date={{ ds }}',
            '--api-key={{ var.value.fxa_amplitude_api_key }}',
            '--secret-key={{ var.value.fxa_amplitude_secret_key }}',
            '--table-id=moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v1',
            '--user-id-field=hmac_user_id',
        ],
        docker_image='gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest',
    )

    wait_for_fxa_delete_events = ExternalTaskCompletedSensor(
        task_id="wait_for_fxa_delete_events",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_delete_events__v1",
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        execution_delta=datetime.timedelta(minutes=30),
        dag=dag,
    )

    wait_for_fxa_delete_events >> shredder_amplitude_fxa
