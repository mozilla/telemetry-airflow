"""
Powers the public https://data.firefox.com/ dashboard.

Source code is in the [firefox-public-data-report-etl repository]
(https://github.com/mozilla/firefox-public-data-report-etl).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.external_task import ExternalTaskSensor
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.dataproc import (
    get_dataproc_parameters,
    moz_dataproc_pyspark_runner,
)
from utils.gcp import bigquery_etl_query
from utils.tags import Tag

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 6),
    "email": [
        "telemetry-alerts@mozilla.com",
        "firefox-hardware-report-feedback@mozilla.com",
        "akomar@mozilla.com",
        "shong@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

tags = [Tag.ImpactTier.tier_3]

dag = DAG(
    "firefox_public_data_report",
    default_args=default_args,
    schedule_interval="0 1 * * MON",
    doc_md=__doc__,
    tags=tags,
)

# hardware_report's execution date will be {now}-7days. It will read last week's main pings,
# therefore we need to wait for yesterday's Main Ping deduplication task to finish
wait_for_main_ping = ExternalTaskSensor(
    task_id="wait_for_main_ping",
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_main_ping",
    execution_delta=timedelta(days=-6),
    check_existence=True,
    mode="reschedule",
    allowed_states=ALLOWED_STATES,
    failed_states=FAILED_STATES,
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)

params = get_dataproc_parameters("google_cloud_airflow_dataproc")

hardware_report = SubDagOperator(
    task_id="public_data_hardware_report",
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        image_version="1.5-debian10",
        dag_name="public_data_hardware_report",
        default_args=default_args,
        cluster_name="public-data-hardware-report-{{ ds }}",
        job_name="firefox-public-data-hardware-report",
        python_driver_code="gs://{}/jobs/moz_dataproc_runner.py".format(
            params.artifact_bucket
        ),
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_metadata={
            "PIP_PACKAGES": "git+https://github.com/mozilla/firefox-public-data-report-etl.git"
        },
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        },
        py_args=[
            "public_data_report",
            "hardware_report",
            "--date_from",
            "{{ ds }}",
            "--bq_table",
            "moz-fx-data-shared-prod.telemetry_derived.public_data_report_hardware",
            "--temporary_gcs_bucket",
            params.storage_bucket,
            "--gcs_bucket",
            "moz-fx-data-static-websit-f7e0-analysis-output",
            "--gcs_path",
            "public-data-report/hardware/",
        ],
        idle_delete_ttl=14400,
        num_workers=2,
        worker_machine_type="n1-standard-4",
        gcp_conn_id=params.conn_id,
        service_account=params.client_email,
        storage_bucket=params.storage_bucket,
    ),
)

wait_for_clients_last_seen = ExternalTaskSensor(
    task_id="wait_for_clients_last_seen",
    external_dag_id="bqetl_main_summary",
    external_task_id="telemetry_derived__clients_last_seen__v1",
    execution_delta=timedelta(days=-6, hours=-1),
    check_existence=True,
    mode="reschedule",
    allowed_states=ALLOWED_STATES,
    failed_states=FAILED_STATES,
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=dag,
)

user_activity = bigquery_etl_query(
    task_id="user_activity",
    destination_table="public_data_report_user_activity_v1",
    project_id="moz-fx-data-shared-prod",
    dataset_id="telemetry_derived",
    dag=dag,
)

user_activity_usage_behavior_export = GKEPodOperator(
    task_id="user_activity_export",
    name="user_activity_export",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/firefox-public-data-report-etl:latest",
    arguments=[
        "-m",
        "public_data_report.cli",
        "user_activity",
        "--bq_table",
        "moz-fx-data-shared-prod.telemetry_derived.public_data_report_user_activity_v1",
        "--gcs_bucket",
        "moz-fx-data-static-websit-f7e0-analysis-output",
        "--gcs_path",
        "public-data-report/user_activity",
    ],
    image_pull_policy="Always",
    dag=dag,
)

annotations_export = GKEPodOperator(
    task_id="annotations_export",
    name="annotations_export",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/firefox-public-data-report-etl:latest",
    arguments=[
        "-m",
        "public_data_report.cli",
        "annotations",
        "--date_to",
        "{{ ds }}",
        "--output_bucket",
        "moz-fx-data-static-websit-f7e0-analysis-output",
        "--output_prefix",
        "public-data-report/annotations",
    ],
    image_pull_policy="Always",
    dag=dag,
)

ensemble_transposer = GKEPodOperator(
    task_id="ensemble_transposer",
    name="ensemble_transposer",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/ensemble-transposer:latest",
    env_vars={
        "GCS_BUCKET_NAME": "moz-fx-data-static-websit-8565-ensemble",
    },
    image_pull_policy="Always",
    dag=dag,
)


wait_for_main_ping >> hardware_report >> ensemble_transposer
wait_for_clients_last_seen >> user_activity >> user_activity_usage_behavior_export
user_activity_usage_behavior_export >> ensemble_transposer
annotations_export >> ensemble_transposer
