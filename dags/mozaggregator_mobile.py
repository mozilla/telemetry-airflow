import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.subdag_operator import SubDagOperator
from utils.dataproc import copy_artifacts_dev, moz_dataproc_pyspark_runner
from utils.status import register_status
from utils.gcp import gke_command

EXPORT_TO_AVRO = True

default_args = {
    "owner": "robhudson@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": [
        "telemetry-alerts@mozilla.com",
        "robhudson@mozilla.com",
        "frank@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("mobile_aggregates", default_args=default_args, schedule_interval="@daily")

subdag_args = default_args.copy()
subdag_args["retries"] = 0

task_id = "mobile_aggregate_view_dataproc"
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
output_bucket = artifact_bucket if is_dev else "moz-fx-data-derived-datasets-parquet"


mobile_aggregate_view_dataproc = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="mobile_aggregates",
        cluster_name="mobile-metrics-aggregates-{{ ds_nodash }}",
        idle_delete_ttl="600",
        num_workers=5,
        worker_machine_type="n1-standard-8",
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        },
        additional_metadata={
            "PIP_PACKAGES": "git+https://github.com/mozilla/python_mozaggregator.git"
        },
        python_driver_code="gs://{}/jobs/mozaggregator_runner.py".format(
            artifact_bucket
        ),
        py_args=[
            "mobile",
            "--date",
            "{{ ds_nodash }}",
            "--output",
            "gs://{}/mobile_metrics_aggregates/v3".format(output_bucket),
            "--num-partitions",
            str(5 * 32),
        ]
        + (
            ["--source", "bigquery", "--project-id", "moz-fx-data-shared-prod"]
            if not EXPORT_TO_AVRO
            else [
                "--source",
                "avro",
                "--avro-prefix",
                "gs://moz-fx-data-derived-datasets-parquet-tmp/avro/mozaggregator/moz-fx-data-shared-prod",
            ]
        ),
        gcp_conn_id=gcp_conn.gcp_conn_id,
        service_account=client_email,
        artifact_bucket=artifact_bucket,
        storage_bucket=storage_bucket,
        default_args=subdag_args,
    ),
)

# export to avro, if necessary
if EXPORT_TO_AVRO:
    gke_command(
        task_id="export_mobile_metrics_avro",
        cmds=["bash"],
        command=[
            "bin/export-avro.sh",
            "moz-fx-data-shared-prod",
            "moz-fx-data-shared-prod:analysis",
            "gs://moz-fx-data-derived-datasets-parquet-tmp/avro/mozaggregator",
            "mobile_metrics_v1",
            '""',
            "{{ ds }}",
        ],
        docker_image="mozilla/python_mozaggregator:latest",
        dag=dag,
    ).set_downstream(mobile_aggregate_view_dataproc)

    gke_command(
        task_id="export_saved_session_avro",
        cmds=["bash"],
        command=[
            "bin/export-avro.sh",
            "moz-fx-data-shared-prod",
            "moz-fx-data-shared-prod:analysis",
            "gs://moz-fx-data-derived-datasets-parquet-tmp/avro/mozaggregator",
            "saved_session_v4",
            "'nightly', 'beta'",
            "{{ ds }}",
        ],
        docker_image="mozilla/python_mozaggregator:latest",
        dag=dag,
    ).set_downstream(mobile_aggregate_view_dataproc)


register_status(
    mobile_aggregate_view_dataproc,
    "Mobile Aggregates",
    "Aggregates of metrics sent through the mobile-events pings.",
)

# copy over artifacts if we're running in dev
if is_dev:
    copy_to_dev = copy_artifacts_dev(dag, project_id, artifact_bucket, storage_bucket)
    copy_to_dev.set_downstream(mobile_aggregate_view_dataproc)
