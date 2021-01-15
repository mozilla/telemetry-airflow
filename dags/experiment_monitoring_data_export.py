from airflow import DAG
from datetime import datetime, timedelta

from utils.gcp import gke_command

from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    'owner': 'ascholtz@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 8),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG('experiment_monitoring_data_export',
         default_args=default_args,
         schedule_interval="*/5 * * * *") as dag:

    wait_for_experiment_enrollment_cumulative_population_estimate = ExternalTaskSensor(
        task_id="wait_for_experiment_enrollment_cumulative_population_estimate",
        external_dag_id="bqetl_experiments_live",
        external_task_id="experiment_enrollment_cumulative_population_estimate",
        check_existence=True,
        dag=dag
    )

    wait_for_experiment_enrollment_other_events_overall = ExternalTaskSensor(
        task_id="wait_for_experiment_enrollment_other_events_overall",
        external_dag_id="bqetl_experiments_live",
        external_task_id="experiment_enrollment_other_events_overall",
        check_existence=True,
        dag=dag
    )

    wait_for_experiment_enrollment_overall = ExternalTaskSensor(
        task_id="wait_for_experiment_enrollment_overall",
        external_dag_id="bqetl_experiments_live",
        external_task_id="telemetry_derived__experiment_enrollment_overall__v1",
        check_existence=True,
        dag=dag
    )

    wait_for_experiment_unenrollment_overall = ExternalTaskSensor(
        task_id="wait_for_experiment_unenrollment_overall",
        external_dag_id="bqetl_experiments_live",
        external_task_id="telemetry_derived__experiment_unenrollment_overall__v1",
        check_existence=True,
        dag=dag
    )

    docker_image = "mozilla/bigquery-etl:latest"
    export_monitoring_data = gke_command(
        task_id="export_monitoring_data",
        command=[
            "script/experiments/export_experiment_monitoring_data",
            "--date", "{{ ds }}"
        ],
        docker_image=docker_image
    )

    export_monitoring_data.set_upstream(wait_for_experiment_enrollment_cumulative_population_estimate)
    export_monitoring_data.set_upstream(wait_for_experiment_unenrollment_overall)
    export_monitoring_data.set_upstream(wait_for_experiment_enrollment_overall)
    export_monitoring_data.set_upstream(wait_for_experiment_enrollment_other_events_overall)
