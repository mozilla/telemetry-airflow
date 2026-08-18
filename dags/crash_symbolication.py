import datetime

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.dataproc import get_dataproc_parameters, moz_dataproc_pyspark_runner
from utils.tags import Tag

"""
### crash symbolication

Two crash report analysis jobs that run on crash data imported from Socorro. Both read
`moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`, which is populated by the
`bqetl_socorro_import` DAG.

The DAG is scheduled daily, but each task only does work on certain weekdays. The
`--run-on-days` argument does the real scheduling, which works around Airflow not
supporting per-task schedules.

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
stability@mozilla.org, and benwu@mozilla.com. Nothing else consumes it.

Impact of failure: A missed run means one missed weekly email.

### top_signatures_correlations (runs Mondays, Wednesdays, Fridays)

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
    "owner": "srose@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "benwu@mozilla.com"
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=30),
}

PIP_PACKAGES = [
    "boto3==1.16.20",
    "scipy==1.5.4",
    "google-cloud-storage==2.7.0",
]

tags = [Tag.ImpactTier.tier_3]

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

    params = get_dataproc_parameters("google_cloud_airflow_dataproc")

    #modules_with_missing_symbols = GKEPodOperator(
    #    task_id="modules_with_missing_symbols",
    #    image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/crash-missing-symbols:latest",
    #    arguments=[
    #        "-m",
    #        "crash_missing_symbols.main",
    #        "--date",
    #        "{{ ds }}",
    #        # Send Mondays only. The report is still built the other six days.
    #        "--run-on-days",
    #        "0",
    #        "--recipient",
    #        "mcastelluccio@mozilla.com",
    #        "--recipient",
    #        "release-mgmt@mozilla.com",
    #        "--recipient",
    #        "stability@mozilla.org",
    #        "--recipient",
    #        "benwu@mozilla.com",
    #    ],
    #    secrets=[ses_aws_access_key_secret, ses_aws_secret_key_secret],
    #    # Failure alerts, unrelated to who the report goes to
    #    # TODO: set these as task defaults after migrating other job
    #    email=["benwu@mozilla.com", "stability@mozilla.org", "telemetry-alerts@mozilla.com"],
    #    dag=dag,
    #)

    top_signatures_correlations = SubDagOperator(
        task_id="top_signatures_correlations",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5-debian10",
            dag_name="top_signatures_correlations",
            default_args=default_args,
            cluster_name="top-signatures-correlations-{{ ds }}-test",
            job_name="top-signatures-correlations",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/benwu/crashcorrelations-update/mozetl/symbolication/top_signatures_correlations.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            },
            py_args=[
                # run monday, wednesday, and friday
                "--run-on-days",
                "0",
                "2",
                "4",
                "--date",
                "{{ ds }}",
                # TEMPORARY, diagnosing the published NULL undercount. Remove this whole
                # block, and count_trace_prod.py, once the cause is known.
                #
                # Counts nulls four ways on the release dataframe and writes them to a
                # separate prefix. --trace-ref must match the ref python_driver_code is
                # fetched from, since the tracer is fetched separately at runtime.
                "--trace-counts",
                "--trace-ref",
                "benwu/crashcorrelations-update",
                "--trace-bucket",
                "benwu-correlations-output",
                # get_versions() fetches the current version from product-details live and
                # ignores --date, so now that 154.0 has shipped the release channel resolves
                # to ['154.0'], which has ~20 crashes against 153.0.4's ~67,000. Pin the
                # versions the Aug 17 run actually read, otherwise there's no data to
                # measure. The driver requires --results-bucket alongside this, since the
                # output no longer matches what the schedule would publish.
                "--override-versions",
                "153.0",
                "153.0.1",
                "153.0.3",
                "153.0.4",
                "--results-bucket",
                "benwu-correlations-output",
            ],
            idle_delete_ttl=14400,
            num_workers=2,
            worker_machine_type="n1-standard-8",
            gcp_conn_id="google_cloud_airflow_dataproc",
            service_account="dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com",
            storage_bucket="moz-fx-data-prod-dataproc-scratch",
        ),
    )

    #wait_for_socorro_import >> modules_with_missing_symbols
    wait_for_socorro_import >> top_signatures_correlations
