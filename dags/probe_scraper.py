import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.weekday import WeekDay
from kubernetes.client import models as k8s

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
# Probe Scraper

*Triage notes*

As long as the most recent DAG run is successful this job can be considered healthy.
In such case, past DAG failures can be ignored.

## Debugging failures

probe_scraper and probe_scraper_moz_central task logs aren't available via the Airflow web console. In
order to access them, go to [GCP Logs Explorer](https://cloudlogging.app.goo.gl/sLyJuaPmVM6SnKtu7).
This link should get you directly to the last 12 hours of probe_scraper pod logs. If necessary, replace
`"probe-scraper.+"` with `"probe-scraper-moz-central.+"` in the query field.
If the above link fails, do the following:

1. Navigate to the [Google Cloud Logging console](https://console.cloud.google.com/logs/query?project=moz-fx-data-airflow-gke-prod)
If you can't access these logs but think you should be able to, [contact Data SRE](https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=DOPS&title=Contacting+Data+SRE).
2. Search for the following, replacing `"probe-scraper.+"` with `"probe-scraper-moz-central.+"` if necessary (make sure to put this in the raw query field - you might need to click the "Show query" button for it to appear):

```
resource.type="k8s_container"
resource.labels.project_id="moz-fx-data-airflow-gke-prod"
resource.labels.location="us-west1"
resource.labels.cluster_name="workloads-prod-v1"
resource.labels.namespace_name="default"
resource.labels.pod_name=~"probe-scraper.+"
severity>=DEFAULT
```

Adjust the time window as needed and you should be able to see logs associated with the failure.

To find a name of the pod related to specific run, navigate to
[probe_scraper DAG in Airflow](https://workflow.telemetry.mozilla.org/tree?dag_id=probe_scraper),
click the task that failed, followed by `View Log`. Here, look for `probe-scraper.[ID]`.
"""

DEFAULT_LOOKML_GENERATOR_IMAGE_VERSION = "v1.17.0"


default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 28),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

aws_access_key_secret = Secret(
    deploy_type="env",
    deploy_target="AWS_ACCESS_KEY_ID",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__aws_access_key",
)
aws_secret_key_secret = Secret(
    deploy_type="env",
    deploy_target="AWS_SECRET_ACCESS_KEY",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__aws_secret_key",
)
mozilla_pipeline_schemas_secret_git_sshkey_b64 = Secret(
    deploy_type="env",
    deploy_target="MPS_SSH_KEY_BASE64",
    secret="airflow-gke-secrets",
    key="probe_scraper_secret__mozilla_pipeline_schemas_secret_git_sshkey_b64",
)

with DAG(
    "probe_scraper",
    doc_md=DOCS,
    default_args=default_args,
    params={"update": Param(True, type="boolean")},
    schedule_interval="0 0 * * *",
    tags=tags,
) as dag:
    airflow_gke_prod_kwargs = {
        "gcp_conn_id": "google_cloud_airflow_gke",
        "project_id": "moz-fx-data-airflow-gke-prod",
        "location": "us-west1",
        "cluster_name": "workloads-prod-v1",
    }

    # Built from repo https://github.com/mozilla/probe-scraper
    probe_scraper_image = "gcr.io/moz-fx-data-airflow-prod-88e0/probe-scraper:latest"

    # probe scraper used to be a single task, but it has beeen split up, and individual
    # failures do not block downstream tasks
    probe_scraper = EmptyOperator(
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
        # Needed for proper cluster autoscaling, because cluster autoscaling
        # works on pod resource requests, instead of usage
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "4500Mi"},
        ),
        # Due to the nature of the container run, we set get_logs to False, to avoid
        # urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes
        # read)' errors where the pod continues to run, but airflow loses its connection
        # and sets the status to Failed
        get_logs=False,
        # Give additional time since the cluster may scale up when running this job
        startup_timeout_seconds=360,
        image=probe_scraper_image,
        arguments=(
            [
                *probe_scraper_base_arguments,
                "--cache-bucket=gs://probe-scraper-prod-cache/",
                "--moz-central",
            ]
        ),
        email=[
            "telemetry-alerts@mozilla.com",
            "telemetry-client-dev@mozilla.com",
            "aplacitelli@mozilla.com",
            "dataops+alerts@mozilla.com",
            "akomar@mozilla.com",
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
                [
                    *probe_scraper_base_arguments,
                    "--glean",
                    f"--glean-url={url}",
                    # if dag param update has been manually set to False, use
                    # "--glean-limit-date=", "--no-update", otherwise default to
                    # "--glean-limit-date={{ds}}", "--update"
                    "--glean-limit-date={{ds if dag_run.conf['update'] else ''}}",
                    "--{{'' if dag_run.conf['update'] else 'no-'}}update",
                ]
                + (
                    [
                        "--bugzilla-api-key",
                        "{{ var.value.bugzilla_probe_expiry_bot_api_key }}",
                    ]
                    if name == "firefox"
                    else []
                )
            ),
            email=[
                "telemetry-alerts@mozilla.com",
                "telemetry-client-dev@mozilla.com",
                "aplacitelli@mozilla.com",
                "dataops+alerts@mozilla.com",
                "akomar@mozilla.com",
            ],
            env_vars={
                "BOTO_PATH": ".gce_boto",
            },
            secrets=[aws_access_key_secret, aws_secret_key_secret],
            dag=dag,
            **airflow_gke_prod_kwargs,
        )
        for name, url in (
            ("firefox", "https://github.com/mozilla-firefox/firefox"),
            ("phabricator", "https://github.com/mozilla-conduit/review"),
            (
                "releases-comm-central",
                "https://github.com/mozilla/releases-comm-central",
            ),
        )
    ]

    probe_scraper_glean >> probe_scraper

    probe_scraper_glean_repositories = GKEPodOperator(
        task_id="probe_scraper_glean_repositories",
        name="probe-scraper-glean-repositories",
        image=probe_scraper_image,
        arguments=(
            [
                *probe_scraper_base_arguments,
                # when --update is specified without --glean-repo or --glean-url,
                # this only writes metadata changes.
                "--update",
                "--glean",
            ]
        ),
        email=[
            "telemetry-alerts@mozilla.com",
            "telemetry-client-dev@mozilla.com",
            "aplacitelli@mozilla.com",
            "dataops+alerts@mozilla.com",
            "akomar@mozilla.com",
        ],
        env_vars={"BOTO_PATH": ".gce_boto"},
        dag=dag,
        **airflow_gke_prod_kwargs,
    )

    probe_scraper_glean_repositories >> probe_scraper_glean

    probe_scraper_checks = [
        GKEPodOperator(
            task_id=f"probe_scraper_{check_name.replace('-', '_')}",
            name=f"probe-scraper-{check_name}",
            image=probe_scraper_image,
            arguments=(
                [
                    *probe_scraper_base_arguments,
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
                "telemetry-alerts@mozilla.com",
                "telemetry-client-dev@mozilla.com",
                "aplacitelli@mozilla.com",
                "dataops+alerts@mozilla.com",
                "akomar@mozilla.com",
            ],
            env_vars={
                "BOTO_PATH": ".gce_boto",
            },
            secrets=[aws_access_key_secret, aws_secret_key_secret],
            dag=dag,
            **airflow_gke_prod_kwargs,
        )
        for check_name in ("check-expiry", "check-fog-expiry")
    ]
    dummy_branch = EmptyOperator(
        task_id="dummy_branch",
        dag=dag,
    )

    class CheckBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            """
            Return an array of task_ids to be executed.

            These tasks must be downstream of the branch task.
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
        email=[
            "akomar@mozilla.com",
            "dataops+alerts@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        task_id="mozilla_schema_generator",
        name="schema-generator-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/mozilla-schema-generator:latest",
        env_vars={
            "MPS_REPO_URL": "git@github.com:mozilla-services/mozilla-pipeline-schemas.git",
            "MPS_BRANCH_SOURCE": "main",
            "MPS_BRANCH_PUBLISH": "generated-schemas",
        },
        secrets=[mozilla_pipeline_schemas_secret_git_sshkey_b64],
        dag=dag,
    )

    schema_generator.set_upstream(probe_scraper)

    probe_expiry_alerts = GKEPodOperator(
        task_id="probe-expiry-alerts",
        name="probe-expiry-alerts",
        image=probe_scraper_image,
        arguments=[
            "python3",
            "-m",
            "probe_scraper.probe_expiry_alert",
            "--date",
            "{{ ds }}",
            "--bugzilla-api-key",
            "{{ var.value.bugzilla_probe_expiry_bot_api_key }}",
        ],
        email=["akomar@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[aws_access_key_secret, aws_secret_key_secret],
        dag=dag,
    )

    probe_expiry_alerts.set_upstream(probe_scraper)

    wait_for_table_partition_expirations = ExternalTaskSensor(
        task_id="wait_for_table_partition_expirations",
        external_dag_id="bqetl_monitoring",
        external_task_id="monitoring_derived__table_partition_expirations__v1",
        execution_delta=timedelta(hours=-2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    ping_expiry_alerts = GKEPodOperator(
        task_id="ping_expiry_alerts",
        image=probe_scraper_image,
        arguments=[
            "python3",
            "-m",
            "probe_scraper.ping_expiry_alert",
            "--run-date",
            "{{ ds }}",
        ],
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[aws_access_key_secret, aws_secret_key_secret],
        dag=dag,
    )

    ping_expiry_alerts.set_upstream(wait_for_table_partition_expirations)
    ping_expiry_alerts.set_upstream(probe_scraper)

    delay_python_task = PythonOperator(
        task_id="wait_for_1_hour", dag=dag, python_callable=lambda: time.sleep(60 * 60)
    )

    probe_scraper >> delay_python_task

    # trigger lookml generation
    trigger_looker = TriggerDagRunOperator(
        task_id="trigger_looker", trigger_dag_id="looker", wait_for_completion=True
    )

    # This emits a POST request to a netlify webhook URL that triggers a new
    # build of the glean dictionary. We do this after the schema generator has
    # finished running as the dictionary uses the new schema files as part of
    # said build.
    glean_dictionary_netlify_build = HttpOperator(
        http_conn_id="http_netlify_build_webhook",
        endpoint=Variable.get("glean_dictionary_netlify_build_webhook_id"),
        method="POST",
        data={},
        owner="jrediger@mozilla.com",
        email=[
            "jrediger@mozilla.com",
            "dataops+alerts@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        task_id="glean_dictionary_build",
        # Glean Dictionary utilizes data from generated LookML namespaces. If Looker DAG fails we want to run the Dictionary build anyway to load updated generated schemas
        trigger_rule="all_done",
        dag=dag,
    )

    delay_python_task >> trigger_looker >> glean_dictionary_netlify_build
