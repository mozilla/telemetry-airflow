import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from utils.dataproc import (
    moz_dataproc_pyspark_runner,
    copy_artifacts_dev,
    get_dataproc_parameters,
)

EXPORT_TO_AVRO = True

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 3, 15),
    "email": [
        "telemetry-alerts@mozilla.com",
        "amiyaguchi@mozilla.com",
        "bmiroglio@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("ltv_daily", default_args=default_args, schedule_interval="@daily")


params = get_dataproc_parameters("google_cloud_airflow_dataproc")

subdag_args = default_args.copy()
subdag_args["retries"] = 0

task_id = "ltv_daily"
ltv_daily = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="ltv-daily",
        cluster_name="ltv-daily-{{ ds_nodash }}",
        idle_delete_ttl="600",
        num_workers=2,
        worker_machine_type="n1-standard-8",
        optional_components=["ANACONDA"],
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        },
        additional_metadata={"PIP_PACKAGES": "lifetimes==0.11.1"},
        python_driver_code="gs://{}/jobs/ltv.py".format(params.artifact_bucket),
        py_args=["--submission-date", "{{ ds }}"],
        gcp_conn_id=params.conn_id,
        service_account=params.client_email,
        artifact_bucket=params.artifact_bucket,
        storage_bucket=params.storage_bucket,
        default_args=subdag_args,
    ),
)

if params.is_dev:
    copy_to_dev = copy_artifacts_dev(
        dag, params.project_id, params.artifact_bucket, params.storage_bucket
    )
    copy_to_dev.set_downstream(ltv_daily)
