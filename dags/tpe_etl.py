from airflow import DAG
from datetime import timedelta, datetime
from operators.emr_spark_operator import EMRSparkOperator
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

default_args = {
    'owner': 'elin@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['elin@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG('taipei_etl',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    schema_generator = GKEPodOperator(
        task_id='taipei_etl',
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location='us-central1-a',
        cluster_name='bq-load-gke-1',
        name='tapei-etl-1',
        namespace='default',
        image='elinmoco/moz-tpe-etl:stable',
        image_pull_policy='Always'
    )
