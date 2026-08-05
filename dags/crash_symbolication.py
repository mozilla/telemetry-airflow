"""
## crash_symbolication

Two crash report analysis jobs that run on crash data imported from Socorro.

Both run as PySpark jobs on ephemeral Dataproc clusters, with the driver code pulled from
the `mozetl/symbolication/` in https://github.com/mozilla/python_mozetl and read from
`moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`, which is populated by
the `bqetl_socorro_import` DAG

The DAG is scheduled daily, but each task only does work on certain weekdays. The
`--run-on-days` argument does the real scheduling: the script exits early when the run
date doesn't match. This works around Airflow not supporting per-task schedules.

### modules_with_missing_symbols (runs Mondays)

Emails a weekly list of modules seen in crash reports that have no debug symbols on the
Mozilla Symbols Server. Missing symbols mean worse stack traces and signatures; the report
tells us which ones to chase down.

Output: Email via AWS SES to mcastelluccio@mozilla.com, release-mgmt@mozilla.com, and
stability@mozilla.org. Nothing else consumes it.

Impact of failure: A missed run means one missed weekly email.

### top_signatures_correlations (runs Mondays, Wednesdays, Fridays)

Finds attributes over-represented in a crash signature compared to all crashes on the
channel, such as a graphics driver version, add-on, or loaded module. Engineers triaging a
top crasher use it to guess at a cause.

Output: gzipped JSON in the moz-fx-data-static-websit-8565-analysis-output GCS bucket,
served at https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/.
Crash Stats reads it from the browser to fill the Correlations tabs on the signature report
and crash report pages (Desktop only).

Impact of failure: user-visible on Crash Stats, but silent. The tabs render an empty panel
rather than an error. Data also goes stale rather than disappearing.
"""

import datetime

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.sensors.external_task import ExternalTaskSensor

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.dataproc import get_dataproc_parameters, moz_dataproc_pyspark_runner
from utils.tags import Tag

default_args = {
    "owner": "srose@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "mcastelluccio@mozilla.com",
        "srose@mozilla.com",
        "telemetry-alerts@mozilla.com",
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

with DAG(
    "crash_symbolication",
    default_args=default_args,
    # dag runs daily but tasks only run on certain days
    schedule_interval="0 5 * * *",
    tags=tags,
    doc_md=__doc__,
) as dag:
    # modules_with_missing_symbols sends results as email via SES
    # these credentials are shared with probe scraper but should be replaced by
    # sendgrid credentials eventually
    ses_aws_conn_id = "aws_prod_probe_scraper"
    ses_access_key, ses_secret_key, _ = AwsBaseHook(
        aws_conn_id=ses_aws_conn_id, client_type="s3"
    ).get_credentials()

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

    modules_with_missing_symbols = SubDagOperator(
        task_id="modules_with_missing_symbols",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5-debian10",
            dag_name="modules_with_missing_symbols",
            default_args=default_args,
            cluster_name="modules-with-missing-symbols-{{ ds }}",
            job_name="modules-with-missing-symbols",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/symbolication/modules_with_missing_symbols.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": ses_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": ses_secret_key,
            },
            py_args=["--run-on-days", "0", "--date", "{{ ds }}"],  # run monday
            idle_delete_ttl=14400,
            num_workers=2,
            worker_machine_type="n1-standard-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    top_signatures_correlations = SubDagOperator(
        task_id="top_signatures_correlations",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5-debian10",
            dag_name="top_signatures_correlations",
            default_args=default_args,
            cluster_name="top-signatures-correlations-{{ ds }}",
            job_name="top-signatures-correlations",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/symbolication/top_signatures_correlations.py",
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
            ],
            idle_delete_ttl=14400,
            num_workers=2,
            worker_machine_type="n1-standard-8",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    wait_for_socorro_import >> modules_with_missing_symbols
    wait_for_socorro_import >> top_signatures_correlations
