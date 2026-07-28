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
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
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

    graphics_trends = GKEPodOperator(
        task_id="graphics_trends",
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/graphics-dashboard:latest",
        arguments=[
            "graphics_dashboard.trends",
            "--output-bucket",
            GCS_BUCKET,
            "--output-prefix",
            GCS_PREFIX,
            "--sample-id-count",
            "1",
            "--billing-project",
            "mozdata",
            "--end-date",
            "{{ ds }}",
            "--start-date",
            "{{ macros.ds_add(ds, -28) }}"
        ],
        dag=dag,
    )

    graphics_dashboard = GKEPodOperator(
        task_id="graphics_dashboard",
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/graphics-dashboard:latest",
        arguments=[
            "graphics_dashboard.dashboard",
            "--output-bucket",
            GCS_BUCKET,
            "--output-prefix",
            GCS_PREFIX,
            "--sample-id-count",
            "1",
            "--billing-project",
            "mozdata",
            "--end-date",
            "{{ ds }}",
            "--time-window",
            "14",
        ],
        dag=dag,
    )

    wait_for_main_ping >> graphics_trends
    wait_for_main_ping >> graphics_dashboard
