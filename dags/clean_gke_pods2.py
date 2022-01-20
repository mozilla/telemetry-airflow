from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### Clean GKE Pods 2

Failures can be ignored during Airflow Triage. This job is idempotent.

Built from cloudops-infra repo, projects/airflow/pod-clean

#### Purpose

This DAG executes a GKEPodOperator to clean out old completed pods
on the shared workloads-prod-v1 gke cluster. We need to do this periodically
because GCP has a 1500 object limit quota.

#### Owner

hwoo@mozilla.com
"""


default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 20),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

dag = DAG("clean-gke-pods2", default_args=default_args, schedule_interval="@daily", doc_md = docs, tags=tags,)

docker_image='gcr.io/moz-fx-data-airflow-prod-88e0/gke-pod-clean:1.4'
gke_cluster_name='workloads-prod-v1'
gke_location='us-west1'
project_id='moz-fx-data-airflow-gke-prod'

# workloads-prod-v1 currently only has a default namespace we care about
docker_args = [
    '--project', project_id,
    '--gke-cluster', gke_cluster_name,
    '-n', 'default',
    '--region', gke_location,
    '--retention-days', '4'
]

clean_gke_pods = GKEPodOperator(
    task_id="clean-gke-pods",
    name='clean-gke-pods',
    image=docker_image,
    gcp_conn_id='google_cloud_airflow_gke',
    project_id=project_id,
    cluster_name=gke_cluster_name,
    location=gke_location,
    arguments=docker_args,
    dag=dag)
