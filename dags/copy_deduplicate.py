import datetime

from airflow import models
from airflow.executors import GetDefaultExecutor
from airflow.operators.subdag_operator import SubDagOperator
from utils.gcp import (bigquery_etl_copy_deduplicate,
                       bigquery_etl_query,
                       gke_command,
                       bigquery_xcom_query,
                       export_to_parquet)

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag_name = "copy_deduplicate"

with models.DAG(
        dag_name,
        schedule_interval="0 1 * * *",
        default_args=default_args) as dag:

    # This single task is responsible for sequentially running copy queries
    # over all the tables in _live datasets into _stable datasets except those
    # that are specifically used in another DAG.
    copy_deduplicate_all = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_all",
        target_project_id="moz-fx-data-shared-prod",
        # Any table listed here under except_tables _must_ have a corresponding
        # copy_deduplicate job in another DAG.
        except_tables=["telemetry_live.main_v4"])


    # Events.

    event_events = bigquery_etl_query(
        task_id="event_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="event_events_v1",
        dataset_id="telemetry_derived",
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    event_events_export = SubDagOperator(
        subdag=export_to_parquet(
            table="moz-fx-data-shared-prod:telemetry_derived.event_events_v1${{ds_nodash}}",
            destination_table="events",
            static_partitions=["submission_date_s3={{ds_nodash}}", "doc_type=event"],
            arguments=[
                "--drop=submission_date",
                "--partition-by=doc_type",
                "--replace=UNIX_TIMESTAMP(timestamp) AS timestamp",
                "--replace=CAST(sample_id AS STRING) AS sample_id",
                "--replace=UNIX_TIMESTAMP(session_start_time) AS session_start_time",
                "--replace=MAP_FROM_ARRAYS(experiments.key, experiments.value.branch) AS experiments",
                "--replace=MAP_FROM_ENTRIES(event_map_values) AS event_map_values",
                "--bigint-columns",
                "sample_id",
                "event_timestamp",
            ],
            # Write into telemetry-test-bucket for a day so we can check schema compat between outputs
            # before turning off the AWS-based event_events dag
            s3_output_bucket="telemetry-test-bucket",
            parent_dag_name=dag.dag_id,
            dag_name="event_events_export",
            default_args=default_args,
            num_workers=10),
        task_id="event_events_export",
        executor=GetDefaultExecutor(),
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
        dag=dag)

    copy_deduplicate_all >> event_events >> event_events_export

    # Experiment enrollment aggregates chain (depends on events)

    experiment_enrollment_aggregates = bigquery_etl_query(
        task_id="experiment_enrollment_aggregates",
        project_id="moz-fx-data-shared-prod",
        destination_table="experiment_enrollment_aggregates_v1",
        dataset_id="telemetry_derived",
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    gen_query_task_id = "experiment_enrollment_aggregates_live_generate_query"

    # setting xcom_push to True outputs this query to an xcom
    experiment_enrollment_aggregates_live_generate_query = gke_command(
        task_id=gen_query_task_id,
        command=[
            "python",
            "templates/telemetry_derived/experiment_enrollment_aggregates_live/view.sql.py",
            "--submission-date",
            "{{ ds }}",
            "--json-output",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        xcom_push=True,
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    experiment_enrollment_aggregates_live_run_query = bigquery_xcom_query(
        task_id="experiment_enrollment_aggregates_live_run_query",
        destination_table=None,
        dataset_id="telemetry_derived",
        xcom_task_id=gen_query_task_id,
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"])

    (event_events >>
     experiment_enrollment_aggregates >>
     experiment_enrollment_aggregates_live_generate_query >>
     experiment_enrollment_aggregates_live_run_query)

    # Daily and last seen views on top of core pings.

    core_clients_daily = bigquery_etl_query(
        task_id='core_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='core_clients_daily_v1',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    core_clients_last_seen = bigquery_etl_query(
        task_id='core_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='core_clients_last_seen_v1',
        dataset_id='telemetry_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    # Daily and last seen views on top of glean pings.

    fenix_clients_daily = bigquery_etl_query(
        task_id='fenix_clients_daily',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_daily_v1',
        dataset_id='org_mozilla_fenix_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    fenix_clients_last_seen = bigquery_etl_query(
        task_id='fenix_clients_last_seen',
        project_id='moz-fx-data-shared-prod',
        destination_table='clients_last_seen_v1',
        dataset_id='org_mozilla_fenix_derived',
        depends_on_past=True,
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    # Aggregated nondesktop tables and their dependency chains.

    firefox_nondesktop_exact_mau28 = bigquery_etl_query(
        task_id='firefox_nondesktop_exact_mau28',
        destination_table='firefox_nondesktop_exact_mau28_raw_v1',
        dataset_id='telemetry',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com', 'pmcdermott@mozilla.com', 'dzielaski@mozilla.com', 'jmundi@mozilla.com'],
    )

    smoot_usage_nondesktop_v2 = bigquery_etl_query(
        task_id='smoot_usage_nondesktop_v2',
        project_id='moz-fx-data-shared-prod',
        destination_table='smoot_usage_nondesktop_v2',
        dataset_id='telemetry_derived',
        email=['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    )

    (copy_deduplicate_all >>
     core_clients_daily >>
     core_clients_last_seen >>
     [firefox_nondesktop_exact_mau28, smoot_usage_nondesktop_v2])

    (copy_deduplicate_all >>
     fenix_clients_daily >>
     fenix_clients_last_seen >>
     [firefox_nondesktop_exact_mau28, smoot_usage_nondesktop_v2])
