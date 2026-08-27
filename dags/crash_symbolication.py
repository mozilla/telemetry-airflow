import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor
from kubernetes.client import models as k8s

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

"""
### crash symbolication

Two crash report analysis jobs that run on crash data imported from Socorro. Both read
`moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`, which is populated by the
`bqetl_socorro_import` DAG.

### crash_missing_symbols (runs daily, emails Mondays)

Emails a weekly list of modules seen in crash reports that have no debug symbols on the
Mozilla Symbols Server. Missing symbols mean worse stack traces and signatures; the report
tells us which ones to chase down.

Runs as a docker-etl container, see
https://github.com/mozilla/docker-etl/tree/main/jobs/crash-missing-symbols. It replaced a
PySpark job that ran on Dataproc, and by default still reproduces two known bugs in that
job so the output can be diffed against it; the `--dedupe-key` and
`--fix-availability-args` flags turn those fixes on.

The report is built in full every day and only sent on Mondays, so breakage surfaces the
day it happens rather than a week later.

Output: Email via AWS SES to mcastelluccio@mozilla.com, release-mgmt@mozilla.com,
and stability@mozilla.org. Nothing else consumes it.

Impact of failure: A missed run means one missed weekly email.

### top_signatures_correlations

Finds attributes over-represented in a crash signature compared to all crashes on the
channel, such as a graphics driver version, add-on, or loaded module. Engineers triaging a
top crasher use it to guess at a cause.

Output: gzipped JSON in the moz-fx-data-static-websit-8565-analysis-output GCS bucket,
served at https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/.
Crash Stats reads it from the browser to fill the Correlations tabs on the signature report
and crash report pages (Desktop only).

Impact of failure: user-visible on Crash Stats, but silent. Data also goes stale rather than disappearing.
"""

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "benwu@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

# The bucket Crash Stats reads. Each run replaces the whole prefix under it.
CORRELATIONS_RESULTS_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"

# SES credentials for crash_missing_symbols, mounted as pod env vars
ses_aws_access_key_secret = Secret(
    deploy_type="env",
    deploy_target="AWS_ACCESS_KEY_ID",
    secret="airflow-gke-restricted-secrets",
    key="probe_scraper_secret__aws_access_key",
)
ses_aws_secret_key_secret = Secret(
    deploy_type="env",
    deploy_target="AWS_SECRET_ACCESS_KEY",
    secret="airflow-gke-restricted-secrets",
    key="probe_scraper_secret__aws_secret_key",
)

with DAG(
    "crash_symbolication",
    default_args=default_args,
    # dag runs daily but tasks only run on certain days
    schedule_interval="0 5 * * *",
    tags=tags,
    doc_md=__doc__,
) as dag:
    wait_for_socorro_import = ExternalTaskSensor(
        task_id="wait_for_socorro_import",
        external_dag_id="bqetl_socorro_import",
        external_task_id="telemetry_derived__socorro_crash__v2",
        check_existence=True,
        execution_delta=datetime.timedelta(hours=1),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    modules_with_missing_symbols = GKEPodOperator(
        task_id="modules_with_missing_symbols",
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/crash-missing-symbols:latest",
        arguments=[
            "-m",
            "crash_missing_symbols.main",
            "--date",
            "{{ ds }}",
            # Send Mondays only. The report is still built the other six days.
            "--run-on-days",
            "0",
            "--recipient",
            "mcastelluccio@mozilla.com",
            "--recipient",
            "release-mgmt@mozilla.com",
            "--recipient",
            "stability@mozilla.org",
        ],
        secrets=[ses_aws_access_key_secret, ses_aws_secret_key_secret],
        dag=dag,
    )

    top_signatures_correlations = GKEPodOperator(
        task_id="top_signatures_correlations",
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/crash-correlations:latest",
        arguments=[
            "-m",
            "crash_correlations.main",
            "--date",
            "{{ ds }}",
            "--results-bucket",
            CORRELATIONS_RESULTS_BUCKET,
            "--no-local",
        ],
        # Measured peak is 2.19Gi on the worst channel
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "2500Mi", "cpu": "1"},
        ),
        dag=dag,
    )

    wait_for_socorro_import >> modules_with_missing_symbols
    wait_for_socorro_import >> top_signatures_correlations
