"""
A job to power graphics dashboard.

Processes main ping data and exports to GCS to power a graphics dashboard at
https://firefoxgraphics.github.io/telemetry/.

This was originally a Databricks notebook that was migrated to a scheduled
Dataproc task. Source code lives in the
[FirefoxGraphics/telemetry](https://github.com/FirefoxGraphics/telemetry)
repository.

This is a overwrite kind of operation and as long as the most recent DAG run succeeded
the job should be considered healthy.
"""

import datetime

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.dataproc import get_dataproc_parameters, moz_dataproc_pyspark_runner
from utils.tags import Tag

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "telemetry-alerts@mozilla.com",
        "benwu@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=20),
}

PIP_PACKAGES = [
    "git+https://github.com/mozilla/python_moztelemetry.git@v0.10.8#egg=python-moztelemetry",
    "git+https://github.com/FirefoxGraphics/telemetry.git#egg=bigquery_shim&subdirectory=analyses/bigquery_shim",
]

GCS_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"
GCS_PREFIX = "gfx/telemetry-data/"

tags = [Tag.ImpactTier.tier_1]

with DAG(
    "graphics_telemetry",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=__doc__,
    tags=tags,
) as dag:
    wait_for_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(hours=2),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    params = get_dataproc_parameters("google_cloud_airflow_dataproc")

    graphics_trends = SubDagOperator(
        task_id="graphics_trends",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5-debian10",
            dag_name="graphics_trends",
            default_args=default_args,
            cluster_name="graphics-trends-{{ ds }}",
            job_name="graphics-trends",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/graphics/graphics_telemetry_trends.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            },
            py_args=[
                "--gcs-bucket",
                GCS_BUCKET,
                "--gcs-prefix",
                GCS_PREFIX,
                "--weekly-fraction",
                "0.003",
            ],
            idle_delete_ttl=14400,
            num_workers=2,
            worker_machine_type="n2-standard-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    graphics_dashboard = SubDagOperator(
        task_id="graphics_dashboard",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5-debian10",
            dag_name="graphics_dashboard",
            default_args=default_args,
            cluster_name="graphics-dashboard-{{ ds }}",
            job_name="graphics-dashboard",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/graphics/graphics_telemetry_dashboard.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            },
            py_args=[
                "--output-bucket",
                GCS_BUCKET,
                "--output-prefix",
                GCS_PREFIX,
                "--release-fraction",
                "0.003",
            ],
            idle_delete_ttl=14400,
            num_workers=2,
            worker_machine_type="n2-highmem-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    wait_for_main_ping >> graphics_trends
    wait_for_main_ping >> graphics_dashboard
