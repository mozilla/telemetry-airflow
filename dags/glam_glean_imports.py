"""
Desktop ETL for importing glean data into GLAM app
"""

from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKENatPodOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "akommasani@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 22),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akommasani@mozilla.com",
        "akomarzewski@mozilla.com",
        "efilho@mozilla.com",
        "linhnguyen@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

dag = DAG(
    "glam_glean_imports",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=__doc__,
    tags=tags,
)

# Make sure all the data for the given day has arrived before running.
wait_for_fenix = ExternalTaskSensor(
    task_id="wait_for_fenix",
    external_dag_id="glam_fenix",
    external_task_id="pre_import",
    execution_delta=timedelta(hours=3),
    check_existence=True,
    mode="reschedule",
    allowed_states=ALLOWED_STATES,
    failed_states=FAILED_STATES,
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)

wait_for_fog = ExternalTaskSensor(
    task_id="wait_for_fog",
    external_dag_id="glam_fog",
    external_task_id="pre_import",
    execution_delta=timedelta(hours=3),
    check_existence=True,
    mode="reschedule",
    allowed_states=ALLOWED_STATES,
    failed_states=FAILED_STATES,
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)

wait_for_glam = ExternalTaskSensor(
    task_id="wait_for_glam",
    external_dag_id="glam",
    external_task_id="extracts",
    execution_delta=timedelta(hours=3),
    check_existence=True,
    mode="reschedule",
    allowed_states=ALLOWED_STATES,
    failed_states=FAILED_STATES,
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)

# Move logic from Glam deployment's GKE Cronjob to this dag for better dependency timing
default_glean_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2022.12.1-37'

base_docker_args = ['/venv/bin/python', 'manage.py']

for env in ['Dev','Prod']:
    glean_import_image = default_glean_import_image
    if env == 'Dev':
        glean_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2022.12.1-37'
    elif env == 'Prod':
        glean_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2022.12.1-37'

    env_vars = dict(
        DATABASE_URL = Variable.get(env + "_glam_secret__database_url"),
        DJANGO_SECRET_KEY = Variable.get(env + "_glam_secret__django_secret_key"),
        # Todo - what does this do?
        DJANGO_CONFIGURATION = env,
        DJANGO_DEBUG = "False",
        DJANGO_SETTINGS_MODULE = "glam.settings",
        GOOGLE_CLOUD_PROJECT = Variable.get(env + "_glam_project"),
    )

    with dag as dag:
        with TaskGroup(group_id=env + '_glean') as glean_env_task_group:
            glam_import_glean_aggs_beta = GKENatPodOperator(
                task_id = 'glam_import_glean_aggs_beta',
                name = 'glam_import_glean_aggs_beta',
                image = glean_import_image,
                arguments = base_docker_args + ['import_glean_aggs', 'beta'],
                env_vars = env_vars,
            )

            glam_import_glean_aggs_nightly = GKENatPodOperator(
                task_id = 'glam_import_glean_aggs_nightly',
                name = 'glam_import_glean_aggs_nightly',
                image = glean_import_image,
                arguments = base_docker_args + ['import_glean_aggs', 'nightly'],
                env_vars = env_vars,
            )

            glam_import_glean_aggs_release = GKENatPodOperator(
                task_id = 'glam_import_glean_aggs_release',
                name = 'glam_import_glean_aggs_release',
                image = glean_import_image,
                arguments = base_docker_args + ['import_glean_aggs', 'release'],
                env_vars = env_vars,
            )

            glam_import_glean_counts = GKENatPodOperator(
                task_id = 'glam_import_glean_counts',
                name = 'glam_import_glean_counts',
                image = glean_import_image,
                arguments = base_docker_args + ['import_glean_counts'],
                env_vars = env_vars,
            )

            # Fan in task dependencies so when both are satisfied, the task group will execute
            # This looks correct in Graph view, but in tree view you will see duplicates
            [wait_for_fenix, wait_for_fog] >> glean_env_task_group



default_glam_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2022.12.1-37'

base_docker_args = ['/venv/bin/python', 'manage.py']

for env in ['Dev','Prod']:
    glam_import_image = default_glam_import_image
    if env == 'Dev':
        glam_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2022.12.1-37'
    elif env == 'Prod':
        glam_import_image = 'gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2022.12.1-37'

    env_vars = dict(
        DATABASE_URL = Variable.get(env + "_glam_secret__database_url"),
        DJANGO_SECRET_KEY = Variable.get(env + "_glam_secret__django_secret_key"),
        DJANGO_CONFIGURATION = env,
        DJANGO_DEBUG = "False",
        DJANGO_SETTINGS_MODULE= "glam.settings",
        GOOGLE_CLOUD_PROJECT = Variable.get(env + "_glam_project"),
    )

    with dag as dag:
        with TaskGroup(group_id=env + '_glam') as glam_env_task_group:
            glam_import_desktop_aggs_beta = GKENatPodOperator(
                task_id = 'glam_import_desktop_aggs_beta',
                name = 'glam_import_desktop_aggs_beta',
                image = glam_import_image,
                arguments = base_docker_args + ['import_desktop_aggs', 'beta'],
                env_vars = env_vars,
            )

            glam_import_desktop_aggs_nightly = GKENatPodOperator(
                task_id = 'glam_import_desktop_aggs_nightly',
                name = 'glam_import_desktop_aggs_nightly',
                image = glam_import_image,
                arguments = base_docker_args + ['import_desktop_aggs', 'nightly'],
                env_vars = env_vars,
            )

            glam_import_desktop_aggs_release = GKENatPodOperator(
                task_id = 'glam_import_desktop_aggs_release',
                name = 'glam_import_desktop_aggs_release',
                image = glam_import_image,
                arguments = base_docker_args + ['import_desktop_aggs', 'release'],
                env_vars = env_vars,
            )

            glam_import_user_counts = GKENatPodOperator(
                task_id = 'glam_import_user_counts',
                name = 'glam_import_user_counts',
                image = glam_import_image,
                arguments = base_docker_args + ['import_user_counts'],
                env_vars = env_vars,
            )

            glam_import_probes = GKENatPodOperator(
                task_id = 'glam_import_probes',
                name = 'glam_import_probes',
                image = glam_import_image,
                arguments = base_docker_args + ['import_probes'],
                env_vars = env_vars,
            )

            wait_for_glam >> glam_env_task_group
