from airflow import DAG
from datetime import datetime, timedelta

from utils.gcp import bigquery_etl_query, gke_command

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

# We rely on max_active_runs=1 at the DAG level to manage the dependency on past runs.
with DAG('experiments_live',
         default_args=default_args,
         concurrency=4,
         max_active_runs=1,
         schedule_interval="*/5 * * * *") as dag:

    docker_image = "mozilla/bigquery-etl:latest"

    experiment_enrollment_aggregates_recents = bigquery_etl_query(
        task_id="experiment_enrollment_aggregates_recents",
        destination_table="experiment_enrollment_aggregates_recents_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        parameters=["submission_timestamp:TIMESTAMP:{{ts}}"],
        dag=dag,
        is_delete_operator_pod=True,
    )

    experiment_search_aggregates_recents = bigquery_etl_query(
        task_id="experiment_search_aggregates_recents",
        destination_table="experiment_search_aggregates_recents_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        parameters=["submission_timestamp:TIMESTAMP:{{ts}}"],
        dag=dag,
        is_delete_operator_pod=True,
    )


    # list of datasets to execute query for and export
    experiment_datasets = [
        "moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_other_events_overall_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_overall_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_unenrollment_overall_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_ad_clicks_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_search_count_v1",
        "moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_search_with_ads_count_v1"
    ]

    export_monitoring_data = gke_command(
        task_id="export_enrollments_monitoring_data",
        command=[
            "python",
            "script/experiments/export_experiment_monitoring_data.py",
            "--datasets"
        ] + experiment_datasets,
        docker_image=docker_image,
        is_delete_operator_pod=True,
    )

    for dataset in experiment_datasets:
        task_id = dataset.split(".")[-1]

        query_etl = bigquery_etl_query(
            task_id=task_id,
            destination_table=task_id,
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            dag=dag,
            is_delete_operator_pod=True,
        )

        query_etl.set_upstream(experiment_enrollment_aggregates_recents)
        query_etl.set_upstream(experiment_search_aggregates_recents)
        export_monitoring_data.set_upstream(query_etl)

    export_daily_active_population_monitoring_data = gke_command(
        task_id=f"export_experiment_enrollment_daily_active_population",
        command=[
            "python",
            "script/experiments/export_experiment_monitoring_data.py",
            "--datasets", "moz-fx-data-shared-prod.telemetry.experiment_enrollment_daily_active_population"
        ],
        docker_image=docker_image
    )
