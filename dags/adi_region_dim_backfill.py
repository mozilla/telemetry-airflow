from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa

DEFAULT_ARGS = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    # TODO set start date, remove end date
    #'start_date': datetime(2018, 1, 1),
    'start_date': datetime(2019, 8, 1),
    'end_date': datetime(2019, 10, 1),
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

# Calculate run month and year
today = date.today()
first_of_month = today.replace(day=1)
last_month_date = first_of_month - timedelta(days=1)
run_month = last_month_date.strftime("%m")
run_year = last_month_date.strftime("%Y")

gcstj_object_conditions = {
    'includePrefixes':  "adi_by_region/year={}/month={}/*.csv".format(run_year, run_month)
}

gcstj_transfer_options = {
    'deleteObjectsUniqueInSink': True
}

# COPY from s3 net-mozaws-data-us-west-2-data-analysis/adi_by_region to gcs
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
    dag=blp_dag
)


# TODO add this back to start testing delete step and load step
'''
# TODO - add step that deletes all records from yr + mnth prior to loading, so we are idempotent

# data looks like
#"_col0","_col1","_col2","city","country_code","domain","ua_family"
#"207","2019","9","AA","US","blocklists.settings.services.mozilla.com","Android"
#"21","2019","9","AA","US","blocklists.settings.services.mozilla.com","Chrome"

load_args = [
    'bq',
    '--location=US',
    'load',
    '--source_format=CSV',
    '--skip_leading_rows=1',
    '--field_delimiter=,',
    'blpadi.adi_by_region',
    'gs://moz-fx-data-derived-datasets-blpadi/adi_by_region/year={}/month={}/*.csv'.format(run_year, run_month),
]

load_bq = GKEPodOperator(
    task_id='bq_load',
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location='us-central1-a',
    cluster_name='bq-load-gke-1',
    name='bq-load',
    namespace='default',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=load_args,
    dag=blp_dag
)
'''
