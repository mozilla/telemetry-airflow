from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 26),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG("clean-pods", default_args=default_args, schedule_interval="@weekly")

gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

gke_location="us-central1-a"
gke_cluster_name="bq-load-gke-1"

# Built from cloudops-infra repo, projects/airflow/pod-clean
# TODO - which project should host the image built? they both work
#docker_image='gcr.io/moz-fx-dataops-images-global/airflow-pod-clean:latest'
docker_image='gcr.io/moz-fx-data-airflow-prod-88e0/airflow-pod-clean:latest'

docker_args = [
    '--project', 'moz-fx-data-derived-datasets',
    '--gke-cluster', gke_cluster_name,
    '--region', gke_location,
    '--retention-days', '14',
    '--dry-run', 'False'
]

clean_pods = GKEPodOperator(
    task_id="clean-pods",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location=gke_location,
    cluster_name=gke_cluster_name,
    name='clean-pods',
    namespace='default',
    image=docker_image,
    arguments=docker_args,
    email=['hwoo@mozilla.com'],
    dag=dag)

