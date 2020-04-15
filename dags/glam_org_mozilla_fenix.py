from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from utils.gcp import gke_command

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 19),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "glam_org_mozilla_fenix", default_args=default_args, schedule_interval="@daily"
)

glam_bucket = "glam-dev-bespoke-nonprod-dataops-mozgcp-net"

wait_for_copy_deduplicate = ExternalTaskSensor(
    task_id="wait_for_copy_deduplicate",
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_all",
    execution_delta=timedelta(hours=-1),
    check_existence=True,
    dag=dag,
)

run_sql = gke_command(
    task_id="run_sql",
    cmds=["bash"],
    env_vars={"DATASET": "glam_etl", "SUBMISSION_DATE": "{{ ds }}"},
    command=["script/glam/run_glam_sql"],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

export_csv = gke_command(
    task_id="export_csv",
    cmds=["bash"],
    env_vars={"DATASET": "glam_etl"},
    command=["script/glam/export_csv"],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

gcs_delete = GoogleCloudStorageDeleteOperator(
    task_id="gcs_delete",
    bucket_name=glam_bucket,
    prefix="glam-extract-fenix",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

gcs_copy = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="gcs_copy",
    source_bucket="glam-fenix-dev",
    source_object="*.csv",
    destination_bucket=glam_bucket,
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

wait_for_copy_deduplicate >> run_sql >> export_csv >> gcs_delete >> gcs_copy
