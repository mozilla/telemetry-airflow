from airflow import DAG
from datetime import datetime, timedelta

from utils.gcp import bigquery_etl_query

from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


with DAG('incline_dashboard',
         default_args=default_args,
         schedule_interval="0 1 * * *") as dag:

    wait_for_baseline_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_baseline_clients_last_seen",
        external_dag_id="copy_deduplicate",
        external_task_id="baseline_clients_last_seen",
    )

    wait_for_core_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_core_clients_last_seen",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
    )

    project = "moz-fx-data-shared-prod"
    dataset = "org_mozilla_firefox_derived"

    migrated_clients = bigquery_etl_query(
        task_id="generate_migrated_clients",
        project_id=project,
        dataset_id=dataset,
        # We recreate this entire table from scratch every day because we are
        # taking the last seen migration ping over all time for each client.
        destination_table=None,
        date_partition_parameter=None,
        sql_file_path="sql/org_mozilla_firefox_derived/migrated_clients_v1/init.sql",
        owner="frank@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"]
    )

    exec_dash = bigquery_etl_query(
        task_id="generate_incline_exec_dash",
        destination_table="incline_executive_v1",
        project_id=project,
        dataset_id=dataset,
        owner="frank@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    )

    gcp_conn_id = 'google_cloud_derived_datasets'
    export_incline_dash = GKEPodOperator(
        task_id="export_incline_dash",
        name="export-incline-dash",
        arguments=["script/export_incline_dash", "{{ ds }}"],
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        namespace="default",
        image="mozilla/bigquery-etl:latest",
    )

    (
        [wait_for_baseline_clients_last_seen, wait_for_core_clients_last_seen] >>
        migrated_clients >>
        exec_dash >>
        export_incline_dash
    )
