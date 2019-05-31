from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator, GKEClusterCreateOperator, GKEClusterDeleteOperator
from utils.gke import create_gke_config

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa

DEFAULT_ARGS = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 24),
    'email': ['hwoo@mozilla.com', 'dataops+alerts@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

cluster_name = 'gke-prio-b'

gcp_conn_id = "google_cloud_prio_b"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

cluster_def = create_gke_config(
    name=cluster_name,
    service_account="prio-runner-b@moz-fx-priotest-project-b.iam.gserviceaccount.com",
    owner_label="hwoo",
    team_label="dataops"
    )

gke_dag = DAG(
    'gke_prio_b',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval='@weekly'
)

create_gke_cluster = GKEClusterCreateOperator(
    task_id='create_gke_cluster',
    project_id=connection.project_id,
    location='us-west1-b',
    gcp_conn_id=gcp_conn_id,
    body=cluster_def,
    dag=gke_dag)


run_prio = GKEPodOperator(
    task_id='run_prio_b',
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location='us-west1-b',
    cluster_name=cluster_name,
    name='run-prio-project-b',
    namespace='default',
    image='mozilla/python-libprio:latest',
    arguments=['scripts/test-cli-integration'],
    dag=gke_dag
)

delete_gke_cluster = GKEClusterDeleteOperator(
    task_id='delete_gke_cluster',
    project_id=connection.project_id,
    location='us-west1-b',
    name=cluster_name,
    gcp_conn_id=gcp_conn_id,
    dag=gke_dag)

create_gke_cluster.set_downstream(run_prio)
run_prio.set_downstream(delete_gke_cluster)
