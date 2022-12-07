import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils.weekday import WeekDay
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag


DOCS = """\
# Probe Scraper

## Debugging failures

probe_scraper and probe_scraper_moz_central task logs aren't available via the Airflow web console. In
order to access them, go to [GCP Logs Explorer](https://cloudlogging.app.goo.gl/qqvCsTbFGCiFmG7L7).
This link should get you directly to the last 12 hours of probe_scraper pod logs. If necessary, replace
`"probe-scraper[.]"` with `"probe-scraper-moz-central[.]"` in the query field.
If the above link fails, do the following:

1. Navigate to the [Google Cloud Logging console](https://console.cloud.google.com/logs/query?project=moz-fx-data-airflow-gke-prod)
If you can't access these logs but think you should be able to, [contact Data SRE](https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=DOPS&title=Contacting+Data+SRE).
2. Search for the following, replacing `"probe-scraper[.]"` with `"probe-scraper-moz-central[.]"` if necessary (make sure to put this in the raw query field - you might need to click the "Show query" button for it to appear):

```
resource.type="k8s_container"
resource.labels.project_id="moz-fx-data-airflow-gke-prod"
resource.labels.location="us-west1"
resource.labels.cluster_name="workloads-prod-v1"
resource.labels.namespace_name="default"
resource.labels.pod_name=~"probe-scraper[.]"
severity>=DEFAULT
```

Adjust the time window as needed and you should be able to see logs associated with the failure.

To find a name of the pod related to specific run, navigate to
[probe_scraper DAG in Airflow](https://workflow.telemetry.mozilla.org/tree?dag_id=probe_scraper),
click the task that failed, followed by `View Log`. Here, look for `probe-scraper.[ID]`.
"""

DEFAULT_LOOKML_GENERATOR_IMAGE_VERSION = "v1.17.0"


default_args = {
    'owner': 'dthorn@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG('probe_scraper',
    doc_md=DOCS,
    default_args=default_args,
    schedule_interval='0 0 * * 1-5',
    tags=tags,
) as dag:

    airflow_gke_prod_kwargs = dict(
        gcp_conn_id="google_cloud_airflow_gke",
        project_id="moz-fx-data-airflow-gke-prod",
        location="us-west1",
        cluster_name="workloads-prod-v1",
    )
    aws_conn_id='aws_prod_probe_scraper'
    aws_access_key, aws_secret_key, session = AwsBaseHook(aws_conn_id=aws_conn_id, client_type='s3').get_credentials()

    # Built from repo https://github.com/mozilla/probe-scraper
    probe_scraper_image='gcr.io/moz-fx-data-airflow-prod-88e0/probe-scraper:latest'

    # probe scraper used to be a single task, but it has beeen split up, and individual
    # failures do not block downstream tasks
    probe_scraper = DummyOperator(
        task_id="probe_scraper",
        trigger_rule="all_done",
        dag=dag,
    )

    probe_scraper_base_arguments = [
        "python3",
        "-m",
        "probe_scraper.runner",
        "--out-dir=/app/probe_data",
        "--cache-dir=/app/probe_cache",
        "--output-bucket=gs://probe-scraper-prod-artifacts/",
        "--env=prod",
    ]

    probe_scraper_moz_central = GKEPodOperator(
        task_id="probe_scraper_moz_central",
        name="probe-scraper-moz-central",
        # Needed to scale the highmem pool from 0 -> 1, because cluster autoscaling
        # works on pod resource requests, instead of usage
        resources={
            "request_memory": "13312Mi",
            "request_cpu": None,
            "limit_memory": "20480Mi",
            "limit_cpu": None,
            "limit_gpu": None,
        },
        # This python job requires 13 GB of memory, thus the highmem node pool
        node_selectors={"nodepool": "highmem"},
        # Due to the nature of the container run, we set get_logs to False, to avoid
        # urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes
        # read)' errors where the pod continues to run, but airflow loses its connection
        # and sets the status to Failed
        get_logs=False,
        # Give additional time since we will likely always scale up when running this job
        startup_timeout_seconds=360,
        image=probe_scraper_image,
        arguments=(
            probe_scraper_base_arguments
            + [
                "--cache-bucket=gs://probe-scraper-prod-cache/",
                "--moz-central",
            ]
        ),
        email=[
            "telemetry-client-dev@mozilla.com",
            "aplacitelli@mozilla.com",
            "hwoo@mozilla.com",
            "relud@mozilla.com",
        ],
        env_vars={"BOTO_PATH": ".gce_boto"},
        dag=dag,
        **airflow_gke_prod_kwargs,
    )

    probe_scraper_moz_central >> probe_scraper

    probe_scraper_glean = [
        GKEPodOperator(
            task_id=f"probe_scraper_glean_{name.replace('-', '_')}",
            name=f"probe-scraper-glean-{name}",
            image=probe_scraper_image,
            arguments=(
                probe_scraper_base_arguments
                + [
                    "--update",
                    "--glean",
                    f"--glean-url={url}",
                    "--glean-limit-date={{ds}}",
                    # don't send emails from these jobs, just print their contents
                    "--dry-run",
                ]
                + (
                    [
                        "--bugzilla-api-key",
                        "{{ var.value.bugzilla_probe_expiry_bot_api_key }}",
                    ]
                    if name == "gecko-dev"
                    else []
                )
            ),
            email=[
                "telemetry-client-dev@mozilla.com",
                "aplacitelli@mozilla.com",
                "hwoo@mozilla.com",
                "relud@mozilla.com",
            ],
            env_vars={"BOTO_PATH": ".gce_boto"},
            dag=dag,
            **airflow_gke_prod_kwargs,
        )
        for name, url in (
            ("gecko-dev", "https://github.com/mozilla/gecko-dev"),
            ("phabricator", "https://github.com/mozilla-conduit/review"),
            (
                "search_engine_usage_study",
                "https://github.com/mozilla-rally/search-engine-usage-study",
            ),
        )
    ]

    probe_scraper_glean >> probe_scraper

    probe_scraper_glean_repositories = GKEPodOperator(
        task_id="probe_scraper_glean_repositories",
        name="probe-scraper-glean-repositories",
        image=probe_scraper_image,
        arguments=(
            probe_scraper_base_arguments
            + [
                # when --update is specified without --glean-repo or --glean-url,
                # this only writes metadata files that are not per glean repo.
                "--update",
                "--glean",
            ]
        ),
        email=[
            "telemetry-client-dev@mozilla.com",
            "aplacitelli@mozilla.com",
            "hwoo@mozilla.com",
            "relud@mozilla.com",
        ],
        env_vars={"BOTO_PATH": ".gce_boto"},
        dag=dag,
        **airflow_gke_prod_kwargs,
    )

    probe_scraper_glean_repositories >> probe_scraper

    probe_scraper_checks = [
        GKEPodOperator(
            task_id=f"probe_scraper_{check_name.replace('-', '_')}",
            name=f"probe-scraper-{check_name}",
            image=probe_scraper_image,
            arguments=(
                probe_scraper_base_arguments
                + [
                    f"--{check_name}",
                    "--bugzilla-api-key={{ var.value.bugzilla_probe_expiry_bot_api_key }}",
                    # don't write any generated files, this job is for emails only
                    "--env=dev",
                    # specify --update without --glean-repo or --glean-url to not scrape any
                    # repos, and download probe data from --output-bucket for expiry checks
                    "--update",
                    "--glean",
                ]
            ),
            email=[
                "telemetry-client-dev@mozilla.com",
                "aplacitelli@mozilla.com",
                "hwoo@mozilla.com",
                "relud@mozilla.com",
            ],
            env_vars={
                "BOTO_PATH": ".gce_boto",
                "AWS_ACCESS_KEY_ID": aws_access_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret_key
            },
            dag=dag,
            **airflow_gke_prod_kwargs,
        )
        for check_name in ("check-expiry", "check-fog-expiry")
    ]
    dummy_branch = DummyOperator(
        task_id="dummy_branch",
        dag=dag,
    )


    class CheckBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            """
            Return an array of task_ids to be executed. These tasks must be
            downstream of the branch task.
            """
            weekday = context["execution_date"].isoweekday()
            if weekday == WeekDay.MONDAY:
                return ["probe_scraper_check_expiry"]
            elif weekday == WeekDay.WEDNESDAY:
                return ["probe_scraper_check_fog_expiry"]
            else:
                return ["dummy_branch"]


    check_branch = CheckBranchOperator(
        task_id="probe_scraper_check_branch",
        # wait for upstream, but ignore upstream failures
        trigger_rule="all_done",
        dag=dag,
    )
    check_branch >> [*probe_scraper_checks, dummy_branch]
    probe_scraper >> check_branch

    schema_generator = GKEPodOperator(
        email=['dthorn@mozilla.com', 'dataops+alerts@mozilla.com'],
        task_id='mozilla_schema_generator',
        name='schema-generator-1',
        image='mozilla/mozilla-schema-generator:latest',
        env_vars={
            "MPS_SSH_KEY_BASE64": "{{ var.value.mozilla_pipeline_schemas_secret_git_sshkey_b64 }}",
            "MPS_REPO_URL": "git@github.com:mozilla-services/mozilla-pipeline-schemas.git",
            "MPS_BRANCH_SOURCE": "main",
            "MPS_BRANCH_PUBLISH": "generated-schemas",
        },
        dag=dag)

    schema_generator.set_upstream(probe_scraper)

    probe_expiry_alerts = GKEPodOperator(
        task_id="probe-expiry-alerts",
        name="probe-expiry-alerts",
        image=probe_scraper_image,
        arguments=[
            "python3", "-m", "probe_scraper.probe_expiry_alert",
            "--date", "{{ ds }}",
            "--bugzilla-api-key", "{{ var.value.bugzilla_probe_expiry_bot_api_key }}"
        ],
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        env_vars={
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key
        },
        dag=dag)

    probe_expiry_alerts.set_upstream(probe_scraper)

    delay_python_task = PythonOperator(
        task_id="wait_for_1_hour",
        dag=dag,
        python_callable=lambda: time.sleep(60 * 60))

    probe_scraper >> delay_python_task

    image_tag = Variable.get("lookml_generator_release_str")
    if image_tag is None:
        image_tag = DEFAULT_LOOKML_GENERATOR_IMAGE_VERSION

    lookml_generator_prod = GKEPodOperator(
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "dataops+alerts@mozilla.com"],
        task_id="lookml_generator",
        name="lookml-generator-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/lookml-generator:" + image_tag,
        startup_timeout_seconds=500,
        dag=dag,
        env_vars={
            "GIT_SSH_KEY_BASE64": Variable.get("looker_repos_secret_git_ssh_key_b64"),
            "HUB_REPO_URL": "git@github.com:mozilla/looker-hub.git",
            "HUB_BRANCH_SOURCE": "base",
            "HUB_BRANCH_PUBLISH": "main",
            "SPOKE_REPO_URL": "git@github.com:mozilla/looker-spoke-default.git",
            "SPOKE_BRANCH_PUBLISH": "main",
            "LOOKER_INSTANCE_URI": "https://mozilla.cloud.looker.com",
            "LOOKER_API_CLIENT_ID": Variable.get("looker_api_client_id_prod"),
            "LOOKER_API_CLIENT_SECRET": Variable.get("looker_api_client_secret_prod"),
            "GITHUB_ACCESS_TOKEN": Variable.get("dataops_looker_github_secret_access_token"),
            "UPDATE_SPOKE_BRANCHES": "true",
        },
        **airflow_gke_prod_kwargs,
    )

    delay_python_task >> lookml_generator_prod

    lookml_generator_staging = GKEPodOperator(
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "dataops+alerts@mozilla.com"],
        task_id="lookml_generator_staging",
        name="lookml-generator-staging-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/lookml-generator:latest",
        dag=dag,
        env_vars={
            "GIT_SSH_KEY_BASE64": Variable.get("looker_repos_secret_git_ssh_key_b64"),
            "HUB_REPO_URL": "git@github.com:mozilla/looker-hub.git",
            "HUB_BRANCH_SOURCE": "base",
            "HUB_BRANCH_PUBLISH": "main-stage",
            "SPOKE_REPO_URL": "git@github.com:mozilla/looker-spoke-default.git",
            "SPOKE_BRANCH_PUBLISH": "main-stage",
            "LOOKER_INSTANCE_URI": "https://mozillastaging.cloud.looker.com",
            "LOOKER_API_CLIENT_ID": Variable.get("looker_api_client_id_staging"),
            "LOOKER_API_CLIENT_SECRET": Variable.get("looker_api_client_secret_staging"),
            "GITHUB_ACCESS_TOKEN": Variable.get("dataops_looker_github_secret_access_token"),
            "UPDATE_SPOKE_BRANCHES": "true",
        },
        **airflow_gke_prod_kwargs,
    )

    delay_python_task >> lookml_generator_staging

    # This emits a POST request to a netlify webhook URL that triggers a new
    # build of the glean dictionary. We do this after the schema generator has
    # finished running as the dictionary uses the new schema files as part of
    # said build.
    glean_dictionary_netlify_build = SimpleHttpOperator(
        http_conn_id="http_netlify_build_webhook",
        endpoint=Variable.get("glean_dictionary_netlify_build_webhook_id"),
        method="POST",
        data={},
        owner="wlach@mozilla.com",
        email=["wlach@mozilla.com", "dataops+alerts@mozilla.com"],
        task_id="glean_dictionary_build",
        dag=dag,
    )

    probe_scraper >> glean_dictionary_netlify_build

