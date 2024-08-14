"""Desktop ETL for importing glean data into GLAM app."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "efilho@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 22),
    "email": [
        "akommasani@mozilla.com",
        "akomarzewski@mozilla.com",
        "efilho@mozilla.com",
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
    schedule_interval="0 19 * * *",
    doc_md=__doc__,
    tags=tags,
)

# Make sure all the data for the given day has arrived before running.
wait_for_fenix = ExternalTaskSensor(
    task_id="wait_for_fenix",
    external_dag_id="glam_fenix",
    external_task_id="pre_import",
    execution_delta=timedelta(hours=17),
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
    execution_delta=timedelta(hours=17),
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
    external_task_group_id="extracts",
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
default_glean_import_image = "gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2023.07.1-43"

base_docker_args = ["/venv/bin/python", "manage.py"]

for env in ["Dev", "Prod"]:
    glean_import_image = default_glean_import_image
    if env == "Dev":  # noqa SIMM114
        glean_import_image = "gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2023.07.1-43"
    elif env == "Prod":
        glean_import_image = "gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2023.07.1-43"

    # Fetch secrets from Google Secret Manager to be injected into the pod.
    database_url_secret = Secret(
        deploy_type="env",
        deploy_target="DATABASE_URL",
        secret="airflow-gke-secrets",
        key=f"{env}_glam_secret__database_url",
    )
    django_secret = Secret(
        deploy_type="env",
        deploy_target="DJANGO_SECRET_KEY",
        secret="airflow-gke-secrets",
        key=f"{env}_glam_secret__django_secret_key",
    )

    env_vars = {
        # Tells Django what set of configs to load depending on the environment. Defaults to dev on the app.
        "DJANGO_CONFIGURATION": env,
        "DJANGO_DEBUG": "False",
        "DJANGO_SETTINGS_MODULE": "glam.settings",
        "GOOGLE_CLOUD_PROJECT": Variable.get(env + "_glam_project"),
    }

    with dag as dag:  # noqa SIM117
        with TaskGroup(group_id=env + "_glean") as glean_env_task_group:
            glam_import_glean_counts = GKEPodOperator(
                reattach_on_restart=True,
                task_id="glam_import_glean_counts",
                name="glam_import_glean_counts",
                image=glean_import_image,
                arguments=[*base_docker_args, "import_glean_counts"],
                env_vars=env_vars,
                secrets=[database_url_secret, django_secret],
            )

            # Fan in task dependencies so when both are satisfied, the task group will execute
            # This looks correct in Graph view, but in tree view you will see duplicates
            [wait_for_fenix, wait_for_fog] >> glean_env_task_group


default_glam_import_image = "gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2023.07.1-43"

base_docker_args = ["/venv/bin/python", "manage.py"]

for env in ["Dev", "Prod"]:
    glam_import_image = default_glam_import_image
    if env == "Dev":  # noqa 114
        glam_import_image = "gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2023.07.1-43"
    elif env == "Prod":
        glam_import_image = "gcr.io/moz-fx-dataops-images-global/gcp-pipelines/glam/glam-production/glam:2023.07.1-43"

    # Fetch secrets from Google Secret Manager to be injected into the pod.
    database_url_secret = Secret(
        deploy_type="env",
        deploy_target="DATABASE_URL",
        secret="airflow-gke-secrets",
        key=f"{env}_glam_secret__database_url",
    )
    django_secret = Secret(
        deploy_type="env",
        deploy_target="DJANGO_SECRET_KEY",
        secret="airflow-gke-secrets",
        key=f"{env}_glam_secret__django_secret_key",
    )

    env_vars = {
        "DJANGO_CONFIGURATION": env,
        "DJANGO_DEBUG": "False",
        "DJANGO_SETTINGS_MODULE": "glam.settings",
        "GOOGLE_CLOUD_PROJECT": Variable.get(env + "_glam_project"),
    }

    with dag as dag, TaskGroup(group_id=env + "_glam") as glam_env_task_group:
        glam_import_user_counts = GKEPodOperator(
            reattach_on_restart=True,
            task_id="glam_import_user_counts",
            name="glam_import_user_counts",
            image=glam_import_image,
            arguments=[*base_docker_args, "import_user_counts"],
            env_vars=env_vars,
            secrets=[database_url_secret, django_secret],
        )

        glam_import_probes = GKEPodOperator(
            reattach_on_restart=True,
            task_id="glam_import_probes",
            name="glam_import_probes",
            image=glam_import_image,
            arguments=[*base_docker_args, "import_probes"],
            env_vars=env_vars,
            secrets=[database_url_secret, django_secret],
        )

        wait_for_glam >> glam_env_task_group
