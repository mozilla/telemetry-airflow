from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa

# The adi_dimensional_by_date bq table is loaded in mango_log_processing.py

DEFAULT_ARGS = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email': ['hwoo@mozilla.com', 'dataops+alerts@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

blp_dag = DAG(
    'adi_by_region',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    # Run on the first of every month, after 1AM PDT. Cron is in UTC
    schedule_interval='0 9 1 * *'
)

gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

aws_conn_id = 'aws_data_iam_blpadi'

# Calculate run month and year. Execution date is the previous period (month)
run_month = '{{ (execution_date.replace(day=1)).strftime("%m") }}'
run_year = '{{ (execution_date.replace(day=1)).strftime("%Y") }}'

gcstj_object_conditions = {
    'includePrefixes':  "adi_by_region/year={}/month={}/".format(run_year, run_month)
}

gcstj_transfer_options = {
    'deleteObjectsUniqueInSink': True
}

# Copy s3://net-mozaws-data-us-west-2-data-analysis/adi_by_region to gcs
s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
    task_id='s3_to_gcs',
    s3_bucket='net-mozaws-data-us-west-2-data-analysis',
    gcs_bucket='moz-fx-data-derived-datasets-blpadi',
    description='adi by region copy from s3 to gcs',
    aws_conn_id=aws_conn_id,
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    object_conditions=gcstj_object_conditions,
    transfer_options=gcstj_transfer_options,
    timeout=720,
    dag=blp_dag
)

# For idempotency, we remove records before loading them
delete_args = [
    'bq',
    '--location=US',
    'query',
    '--use_legacy_sql=false',
    "DELETE from blpadi.adi_by_region WHERE yr = {} AND mnth = {}".format(run_year, run_month)
]

delete_from_table = GKEPodOperator(
    task_id='delete_from_adi_by_region',
    name='delete-from-adi-by-region-table',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=delete_args,
    dag=blp_dag
)

# Finally we load the csv into bq
load_args = [
    'bq',
    '--location=US',
    'load',
    '--source_format=CSV',
    '--skip_leading_rows=1',
    '--field_delimiter=,',
    'blpadi.adi_by_region',
    "gs://moz-fx-data-derived-datasets-blpadi/adi_by_region/year={}/month={}/*.csv".format(run_year, run_month)
]

load_bq = GKEPodOperator(
    task_id='bq_load_adi_by_region',
    name='bq-load-adi-by-region',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=load_args,
    dag=blp_dag
)

s3_to_gcs >> delete_from_table >> load_bq
