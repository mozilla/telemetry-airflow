from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
# Looker

*Triage notes*

As long as the most recent DAG run is successful this job can be considered healthy.
In such case, past DAG failures can be ignored.

- Failure of the `lookml_generator` task may be due to a new Glean app or changes to
`custom-namespaces.yaml`. In these cases, the task will have created a PR in
[looker-spoke-default](https://github.com/mozilla/looker-spoke-default)
with the title "Auto-push from LookML Generator". These PRs will need to be merged
and the task re-run.
"""

DEFAULT_LOOKML_GENERATOR_IMAGE_VERSION = "v1.17.0"


default_args = {
    "owner": "ascholtz@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 21),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

looker_repos_secret_git_ssh_key_b64 = Secret(
    deploy_type="env",
    deploy_target="GIT_SSH_KEY_BASE64",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_repos_secret_git_ssh_key_b64",
)
looker_api_client_id_prod = Secret(
    deploy_type="env",
    deploy_target="LOOKER_API_CLIENT_ID",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_id_prod",
)
looker_api_client_secret_prod = Secret(
    deploy_type="env",
    deploy_target="LOOKER_API_CLIENT_SECRET",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_secret_prod",
)
looker_api_client_id_staging = Secret(
    deploy_type="env",
    deploy_target="LOOKER_API_CLIENT_ID",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_id_staging",
)
looker_api_client_secret_staging = Secret(
    deploy_type="env",
    deploy_target="LOOKER_API_CLIENT_SECRET",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__looker_api_client_secret_staging",
)
dataops_looker_github_secret_access_token = Secret(
    deploy_type="env",
    deploy_target="GITHUB_ACCESS_TOKEN",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__dataops_looker_github_secret_access_token",
)


with DAG(
    "looker",
    doc_md=DOCS,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    tags=tags,
) as dag:
    airflow_gke_prod_kwargs = {
        "gcp_conn_id": "google_cloud_airflow_gke",
        "project_id": "moz-fx-data-airflow-gke-prod",
        "location": "us-west1",
        "cluster_name": "workloads-prod-v1",
    }

    image_tag = Variable.get("lookml_generator_release_str")
    if image_tag is None:
        image_tag = DEFAULT_LOOKML_GENERATOR_IMAGE_VERSION

    lookml_generator_prod = GKEPodOperator(
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "dataops+alerts@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        task_id="lookml_generator",
        name="lookml-generator-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/lookml-generator:" + image_tag,
        startup_timeout_seconds=500,
        dag=dag,
        env_vars={
            "HUB_REPO_URL": "git@github.com:mozilla/looker-hub.git",
            "HUB_BRANCH_SOURCE": "base",
            "HUB_BRANCH_PUBLISH": "main",
            "SPOKE_REPO_URL": "git@github.com:mozilla/looker-spoke-default.git",
            "SPOKE_BRANCH_PUBLISH": "main",
            "LOOKER_INSTANCE_URI": "https://mozilla.cloud.looker.com",
            "UPDATE_SPOKE_BRANCHES": "true",
        },
        secrets=[
            looker_repos_secret_git_ssh_key_b64,
            looker_api_client_id_prod,
            looker_api_client_secret_prod,
            dataops_looker_github_secret_access_token,
        ],
        **airflow_gke_prod_kwargs,
    )

    lookml_generator_staging = GKEPodOperator(
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "dataops+alerts@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        task_id="lookml_generator_staging",
        name="lookml-generator-staging-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/lookml-generator:latest",
        dag=dag,
        env_vars={
            "HUB_REPO_URL": "git@github.com:mozilla/looker-hub.git",
            "HUB_BRANCH_SOURCE": "base",
            "HUB_BRANCH_PUBLISH": "main-stage",
            "SPOKE_REPO_URL": "git@github.com:mozilla/looker-spoke-default.git",
            "SPOKE_BRANCH_PUBLISH": "main-stage",
            "LOOKER_INSTANCE_URI": "https://mozillastaging.cloud.looker.com",
            "UPDATE_SPOKE_BRANCHES": "true",
        },
        secrets=[
            looker_repos_secret_git_ssh_key_b64,
            looker_api_client_id_staging,
            looker_api_client_secret_staging,
            dataops_looker_github_secret_access_token,
        ],
        **airflow_gke_prod_kwargs,
    )

    lookml_generator_staging >> lookml_generator_prod
