import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.subdag_operator import SubDagOperator
from utils.dataproc import moz_dataproc_pyspark_runner

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2019, 12, 1),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "prerelease_telemetry_aggregates_cloudsql",
    default_args=default_args,
    schedule_interval="@daily",
)


subdag_args = default_args.copy()
subdag_args["retries"] = 0

task_id = "prerelease_telemetry_aggregate_view_dataproc_cloudsql"
gcp_conn = GoogleCloudBaseHook("google_cloud_airflow_dataproc")
client_email = "dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com"
artifact_bucket = "moz-fx-data-prod-airflow-dataproc-artifacts"
storage_bucket = "moz-fx-data-prod-dataproc-scratch"


task_id = "prerelease_telemetry_aggregate_view_dataproc_cloudsql"
# process via dataproc and write to cloudsql
prerelease_telemetry_aggregate_view_dataproc_cloudsql = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="prerelease_aggregates_cloudsql",
        cluster_name="prerelease-telemetry-aggregates-cloudsql-{{ ds_nodash }}",
        idle_delete_ttl="600",
        num_workers=10,
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
            "{{ var.value.mozaggregator_cloudsql_pass }}",
            "--postgres-host",
            "{{ var.value.mozaggregator_cloudsql_host }}",
            "--postgres-ro-host",
            "{{ var.value.mozaggregator_cloudsql_ro_host }}",
            "--num-partitions",
            str(10 * 32),
            "--source",
            "bigquery",
            "--project-id",
            "moz-fx-data-shared-prod",
        ],
        gcp_conn_id=gcp_conn.gcp_conn_id,
        service_account=client_email,
        artifact_bucket=artifact_bucket,
        storage_bucket=storage_bucket,
        default_args=subdag_args,
    ),
)
