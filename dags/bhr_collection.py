"""
A processing job on top of BHR pings.

Migrated from Databricks and now running as a scheduled Dataproc task.

BHR is related to the Background Hang Monitor in desktop Firefox.
See [bug 1675103](https://bugzilla.mozilla.org/show_bug.cgi?id=1675103)

The [job source](https://github.com/mozilla/python_mozetl/blob/main/mozetl/bhr_collection)
is maintained in the mozetl repository.
"""

import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from operators.gcp_container_operator import GKEPodOperator
from utils.dataproc import get_dataproc_parameters, moz_dataproc_pyspark_runner
from utils.tags import Tag

default_args = {
    "owner": "kik@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "telemetry-alerts@mozilla.com",
        "kik@mozilla.com",
        "dothayer@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG(
    "bhr_collection",
    default_args=default_args,
    schedule=[Dataset("//copy_deduplicate/copy_deduplicate_all")],
    doc_md=__doc__,
    tags=tags,
) as dag:
    # Jobs read from/write to s3://telemetry-public-analysis-2/bhr/data/hang_aggregates/
    write_aws_conn_id = "aws_dev_telemetry_public_analysis_2_rw"
    aws_access_key, aws_secret_key, _ = AwsBaseHook(
        aws_conn_id=write_aws_conn_id, client_type="s3"
    ).get_credentials()

    params = get_dataproc_parameters("google_cloud_airflow_dataproc")

    bhr_collection = SubDagOperator(
        task_id="bhr_collection",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5-debian10",
            dag_name="bhr_collection",
            default_args=default_args,
            cluster_name="bhr-collection-{{ ds }}",
            job_name="bhr-collection",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/bhr_collection/bhr_collection.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": "boto3==1.16.20 click==7.1.2"},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": aws_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": aws_secret_key,
            },
            py_args=[
                "--date",
                "{{ ds }}",
                "--sample-size",
                "0.5",
            ],
            idle_delete_ttl=14400,
            num_workers=6,
            worker_machine_type="n1-highmem-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    # TODO remove this when the job writes to GCS directly
    gcs_sync = GKEPodOperator(
        task_id="s3_gcs_sync",
        name="s3-gcs-sync",
        image="google/cloud-sdk:435.0.1-alpine",
        arguments=[
            "/google-cloud-sdk/bin/gsutil",
            "-m",
            "rsync",
            "-d",
            "-r",
            "s3://telemetry-public-analysis-2/bhr/",
            "gs://moz-fx-data-static-websit-8565-analysis-output/bhr/",
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
        },
        dag=dag,
    )

    bhr_collection >> gcs_sync
