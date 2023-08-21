from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### Clean GKE Pods

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
    "owner": "hwoo@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 26),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

dag = DAG(
    "clean-gke-pods",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
)

# docker_image = 'us-west1-docker.pkg.dev/moz-fx-data-airflow-prod-88e0/data-science-artifacts/gke-pod-clean:1.3'
docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/gke-pod-clean:1.4"
gke_cluster_name = "workloads-prod-v1"
gke_location = "us-west1"
project_id = "moz-fx-data-airflow-gke-prod"

docker_args = [
    "--project",
    project_id,
    "--gke-cluster",
    gke_cluster_name,
    "--region",
    gke_location,
    "--retention-days",
    "4",
]

clean_gke_pods = GKEPodOperator(
    task_id="clean-gke-pods",
    name="clean-gke-pods",
    image=docker_image,
    arguments=docker_args,
    dag=dag,
)
