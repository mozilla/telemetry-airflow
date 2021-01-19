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
}

with DAG('experiment_monitoring_data_export',
         default_args=default_args,
         schedule_interval="*/5 * * * *") as dag:

    docker_image = "mozilla/bigquery-etl:latest"

    # list of datasets to export
    # upstream_task is optional and refers to an upstream task dependency in bqetl_experiments_live
    export_tasks = [
        {
            "upstream_task": "experiment_enrollment_other_events_overall",
            "dataset": "moz-fx-data-shared-prod.telemetry.experiment_enrollment_other_events_overall"
        },
        {
            "dataset": "moz-fx-data-shared-prod.telemetry.experiment_enrollment_daily_active_population"
        },
        {
            "upstream_task": "experiment_enrollment_cumulative_population_estimate",
            "dataset": "moz-fx-data-shared-prod.telemetry.experiment_enrollment_cumulative_population_estimate"
        },
        {
            "upstream_task": "telemetry_derived__experiment_enrollment_overall__v1",
            "dataset": "moz-fx-data-shared-prod.telemetry.experiment_enrollment_overall"
        },
        {
            "upstream_task": "telemetry_derived__experiment_unenrollment_overall__v1",
            "dataset": "moz-fx-data-shared-prod.telemetry.experiment_enrollment_unenrollment_overall"
        }
    ]

    for export_task in export_tasks:
        task_id = export_task["dataset"].split(".")[-1]

        # separate export tasks that run in parallel to speedup the whole export process
        export_monitoring_data = gke_command(
            task_id=f"export_{task_id}",
            command=[
                "python",
                "script/experiments/export_experiment_monitoring_data.py",
                "--date", "{{ ds }}"
                "--datasets", export_task["dataset"]
            ],
            docker_image=docker_image
        )

        if "upstream_task" in export_task:
            wait_for_upstream = ExternalTaskSensor(
                task_id=f"wait_for_{task_id}",
                external_dag_id="bqetl_experiments_live",
                external_task_id=export_task["upstream_task"],
                check_existence=True,
                dag=dag
            )
   
            export_monitoring_data.set_upstream(wait_for_upstream)
