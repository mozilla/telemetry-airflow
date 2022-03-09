"""
Desktop ETL for importing glam data into GLAM app
"""

from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKENatPodOperator
from operators.task_sensor import ExternalTaskCompletedSensor
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator

from glam_subdags.extract import extracts_subdag, extract_user_counts
from glam_subdags.histograms import histogram_aggregates_subdag
from glam_subdags.general import repeated_subdag
from glam_subdags.generate_query import generate_and_run_desktop_query
from utils.gcp import bigquery_etl_query, gke_command
from utils.tags import Tag

default_args = {
    "owner": "akommasani@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 22),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akommasani@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}



tags = [Tag.ImpactTier.tier_2]

dag = DAG(
    "glam_imports",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    doc_md=__doc__,
    tags=tags,
)


# Make sure all the data for the given day has arrived before running.
wait_for_glam = ExternalTaskCompletedSensor(
    task_id="wait_for_glam",
    external_dag_id="glam",
    external_task_id="pre_import",
    execution_delta=timedelta(hours=3),
    check_existence=True,
    mode="reschedule",
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)


# Move logic from Glam deployment's GKE Cronjob to this dag for better dependency timing
glam_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2021.8.1-10'

base_docker_args = ['/venv/bin/python', 'manage.py']

env_vars = dict(
    DATABASE_URL = Variable.get("glam_secret__database_url"),
    DJANGO_SECRET_KEY = Variable.get("glam_secret__django_secret_key"),
    DJANGO_CONFIGURATION = "Prod",
    DJANGO_DEBUG = "False",
    DJANGO_SETTINGS_MODULE = "glam.settings",
    GOOGLE_CLOUD_PROJECT = "moz-fx-data-glam-prod-fca7"
)

glam_import_desktop_aggs_beta = GKENatPodOperator(
    task_id = 'glam_import_desktop_aggs_beta',
    name = 'glam_import_desktop_aggs_beta',
    image = glam_import_image,
    arguments = base_docker_args + ['import_desktop_aggs', 'beta'],
    env_vars = env_vars,
    dag=dag)

glam_import_desktop_aggs_nightly = GKENatPodOperator(
    task_id = 'glam_import_desktop_aggs_nightly',
    name = 'glam_import_desktop_aggs_nightly',
    image = glam_import_image,
    arguments = base_docker_args + ['import_desktop_aggs', 'nightly'],
    env_vars = env_vars,
    dag=dag)

glam_import_desktop_aggs_release = GKENatPodOperator(
    task_id = 'glam_import_desktop_aggs_release',
    name = 'glam_import_desktop_aggs_release',
    image = glam_import_image,
    arguments = base_docker_args + ['import_desktop_aggs', 'release'],
    env_vars = env_vars,
    dag=dag)

glam_import_user_counts = GKENatPodOperator(
    task_id = 'glam_import_user_counts',
    name = 'glam_import_user_counts',
    image = glam_import_image,
    arguments = base_docker_args + ['import_user_counts'],
    env_vars = env_vars,
    dag=dag)

glam_import_probes = GKENatPodOperator(
    task_id = 'glam_import_probes',
    name = 'glam_import_probes',
    image = glam_import_image,
    arguments = base_docker_args + ['import_probes'],
    env_vars = env_vars,
    dag=dag)

[wait_for_glam] >> glam_import_desktop_aggs_beta
[wait_for_glam] >> glam_import_desktop_aggs_nightly
[wait_for_glam] >> glam_import_desktop_aggs_release
[wait_for_glam] >> glam_import_user_counts
[wait_for_glam] >> glam_import_probes
