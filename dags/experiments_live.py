"""
See [experiments-monitoring-data-export in the docker-etl repository]
(https://github.com/mozilla/docker-etl/tree/main/jobs/experiments-monitoring-data-export).

This DAG exports views related to experiment monitoring to GCS as JSON
every 5 minutes to power the Experimenter console.
"""

from airflow import DAG
from datetime import datetime, timedelta

from utils.gcp import bigquery_etl_query, gke_command

from airflow.sensors.external_task import ExternalTaskSensor
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    'owner': 'ascholtz@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 8),
    'email_on_failure': True,
    'email_on_retry': True,
}

# We rely on max_active_runs=1 at the DAG level to manage the dependency on past runs.
with DAG('experiments_live',
         default_args=default_args,
         # Will be renamed to max_active_tasks sometime later as main upstream branch states
         # max_active_tasks=4,
         concurrency=4,
         max_active_runs=1,
         schedule_interval="*/5 * * * *",
         doc_md=__doc__,
) as dag:

    # list of datasets to export data to GCS
    experiment_datasets = [
        "moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_other_events_overall_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_overall_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_unenrollment_overall_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_ad_clicks_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_search_count_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_search_with_ads_count_v1",
        "moz-fx-data-shared-prod.telemetry.experiment_enrollment_daily_active_population"
    ]

    experiment_enrollment_export = gke_command(
        task_id="experiment_enrollment_export",
        command=[
            "python", "experiments_monitoring_data_export/export.py",
            "--datasets",
        ] + experiment_datasets,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/experiments-monitoring-data-export_docker_etl:latest",
        dag=dag,
    )
