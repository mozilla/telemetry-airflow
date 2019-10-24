from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator


DEFAULT_ARGS = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'end_date': datetime(2019, 10, 1),
    'email': ['hwoo@mozilla.com', 'dataops+alerts@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

blp_dag = DAG(
    'adi_region_dim_backfill',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval='0 0 * * *'
)




# TODO - create bq table first  -> maybe do a 1 shot single csv load into the table, then 
# start loading from s3 net-mozaws-data-us-west-2-data-analysis/adi_by_region
# actually dont partition at all
# yr,mnth,region,country_code,domain,tot_requests,product
# 2018,1,AA,US,blocklists.settings.services.mozilla.com,89,Chrome
# 2018,1,AA,US,blocklists.settings.services.mozilla.com,1807,Firefox
load_args = [
    'bq',
    '--location=US',
    'load',
    '--source_format=CSV',
    '--skip_leading_rows=1',
    '--replace',
    '--field_delimiter=,',
    'blpadi.adi_by_region${{ dont partition at all }}',
    'gs://dp2-stage-vertica/adi_by_region/adi_by_region-{{ dont partition at all?? }}.csv',
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

