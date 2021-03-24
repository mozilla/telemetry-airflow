from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

docs = """
### Clean GKE Sandboxes

Built from cloudops-infra repo, projects/airflow/gke-sandbox-clean

#### Purpose

This DAG executes a GKEPodOperator to clean out sandbox gke clusters
in the moz-fx-data-gke-sandbox gcp project. These clusters are created
by the Makefile's `make gke` target.

#### Owner

hwoo@mozilla.com
"""

default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 24),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG("clean-gke-sandbox", default_args=default_args, schedule_interval="@daily", doc_md = docs)

docker_image='gcr.io/moz-fx-data-airflow-prod-88e0/gke-sandbox-clean:1.0'
gke_cluster_name='bq-load-gke-1'
gke_location='us-central1-a'

docker_args = [
    '--project', 'moz-fx-data-gke-sandbox',
    '--retention-days', '4'
]

clean_gke_sandbox = GKEPodOperator(
    task_id="clean-gke-sandbox",
    name='clean-gke-sandbox',
    image=docker_image,
    arguments=docker_args,
    dag=dag)
