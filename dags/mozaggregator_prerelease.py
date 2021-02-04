import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.gcs_delete_operator import (
    GoogleCloudStorageDeleteOperator,
)
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.subdag_operator import SubDagOperator
from utils.dataproc import moz_dataproc_pyspark_runner, copy_artifacts_dev
from utils.gcp import gke_command

EXPORT_TO_AVRO = True

default_args = {
    "owner": "frank@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2018, 12, 23),
    "email": [
        "telemetry-alerts@mozilla.com",
        "frank@mozilla.com",
        "amiyaguchi@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "prerelease_telemetry_aggregates",
    default_args=default_args,
    schedule_interval="@daily",
)


subdag_args = default_args.copy()
subdag_args["retries"] = 0

task_id = "prerelease_telemetry_aggregate_view_dataproc"
gcp_conn = GoogleCloudBaseHook("google_cloud_airflow_dataproc")
keyfile = json.loads(gcp_conn.extras["extra__google_cloud_platform__keyfile_dict"])
project_id = keyfile["project_id"]

is_dev = os.environ.get("DEPLOY_ENVIRONMENT") == "dev"
client_email = (
    keyfile["client_email"]
    if is_dev
    else "dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com"
)
artifact_bucket = (
    "{}-dataproc-artifacts".format(project_id)
    if is_dev
    else "moz-fx-data-prod-airflow-dataproc-artifacts"
)
storage_bucket = (
    "{}-dataproc-scratch".format(project_id)
    if is_dev
    else "moz-fx-data-prod-dataproc-scratch"
)


prerelease_telemetry_aggregate_view_dataproc = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="prerelease_aggregates",
        cluster_name="prerelease-telemetry-aggregates-{{ ds_nodash }}",
        idle_delete_ttl="600",
        num_workers=10,
        worker_machine_type="n1-standard-8",
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar",
            "spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.4",
        },
        additional_metadata={
            "PIP_PACKAGES": "git+https://github.com/mozilla/python_mozaggregator.git"
        },
        python_driver_code="gs://{}/jobs/mozaggregator_runner.py".format(
            artifact_bucket
        ),
        py_args=[
            "aggregator",
            "--date",
            "{{ ds_nodash }}",
            "--channels",
            "nightly,aurora,beta",
            "--postgres-db",
            "telemetry",
            "--postgres-user",
            "root",
            "--postgres-pass",
            "{{ var.value.mozaggregator_postgres_pass }}",
            "--postgres-host",
            "{{ var.value.mozaggregator_postgres_host }}",
            "--postgres-ro-host",
            "{{ var.value.mozaggregator_postgres_ro_host }}",
            "--num-partitions",
            str(10 * 32),
        ]
        + (
            ["--source", "bigquery", "--project-id", "moz-fx-data-shared-prod"]
            if not EXPORT_TO_AVRO
            else [
                "--source",
                "avro",
                "--avro-prefix",
                "gs://moz-fx-data-derived-datasets-parquet-tmp/avro/mozaggregator/prerelease/moz-fx-data-shared-prod",
            ]
        ),
        gcp_conn_id=gcp_conn.gcp_conn_id,
        service_account=client_email,
        artifact_bucket=artifact_bucket,
        storage_bucket=storage_bucket,
        default_args=subdag_args,
    ),
)

trim_database = gke_command(
    task_id="trim_database",
    cmds=["bash"],
    command=[
        "python",
        "-m",
        "mozaggregator.trim_db",
        "--retention-period",
        f"{365*2}",  # 2 year retention
        "--postgres-db",
        "telemetry",
        "--postgres-user",
        "root",
        "--postgres-pass",
        "{{ var.value.mozaggregator_postgres_pass }}",
        "--postgres-host",
        "{{ var.value.mozaggregator_postgres_host }}",
        # TODO: uncomment this after a successful run
        # "--no-dry-run",
    ],
    docker_image="mozilla/python_mozaggregator:latest",
    dag=dag,
)

prerelease_telemetry_aggregate_view_dataproc >> trim_database

# export to avro, if necessary
if EXPORT_TO_AVRO:
    gke_command(
        task_id="export_main_avro",
        cmds=["bash"],
        command=[
            "bin/export-avro.sh",
            "moz-fx-data-shared-prod",
            "moz-fx-data-shared-prod:analysis",
            "gs://moz-fx-data-derived-datasets-parquet-tmp/avro/mozaggregator/prerelease",
            "main_v4",
            "'nightly', 'beta'",
            "{{ ds }}",
        ],
        docker_image="mozilla/python_mozaggregator:latest",
        dag=dag,
    ).set_downstream(prerelease_telemetry_aggregate_view_dataproc)

    # Delete the GCS data
    GoogleCloudStorageDeleteOperator(
        task_id="delete_main_avro",
        bucket_name="moz-fx-data-derived-datasets-parquet-tmp",
        prefix="avro/mozaggregator/prerelease/moz-fx-data-shared-prod/{{ ds_nodash }}/main_v4",
        google_cloud_storage_conn_id=gcp_conn.gcp_conn_id,
        dag=dag,
    ).set_upstream(prerelease_telemetry_aggregate_view_dataproc)

# copy over artifacts if we're running in dev
if is_dev:
    copy_to_dev = copy_artifacts_dev(dag, project_id, artifact_bucket, storage_bucket)
    copy_to_dev.set_downstream(prerelease_telemetry_aggregate_view_dataproc)
