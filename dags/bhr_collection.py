import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator

from utils.dataproc import moz_dataproc_pyspark_runner, get_dataproc_parameters

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "telemetry-alerts@mozilla.com",
        "bewu@mozilla.com",
        "dothayer@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=30),
}

with DAG(
        "bhr_collection",
        default_args=default_args,
        schedule_interval="0 5 * * *",
) as dag:
    # Jobs read from/write to s3://telemetry-public-analysis-2/bhr/data/hang_aggregates/
    write_aws_conn_id = 'aws_dev_telemetry_public_analysis_2_rw'
    aws_access_key, aws_secret_key, _ = AwsHook(write_aws_conn_id).get_credentials()

    wait_for_bhr_ping = ExternalTaskSensor(
        task_id="wait_for_bhr_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(hours=4),
        check_existence=True,
        dag=dag,
    )

    params = get_dataproc_parameters("google_cloud_airflow_dataproc")

    bhr_collection = SubDagOperator(
        task_id="bhr_collection",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5",
            dag_name="bhr_collection",
            default_args=default_args,
            cluster_name="bhr-collection-{{ ds }}",
            job_name="bhr-collection",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/bhr_collection/bhr_collection.py",
            init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
            additional_metadata={"PIP_PACKAGES": "boto3==1.16.20 click==7.1.2"},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": aws_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": aws_secret_key
            },
            py_args=[
                "--date", "{{ ds }}",
                "--sample-size", "0.5",
            ],
            idle_delete_ttl="14400",
            num_workers=5,
            worker_machine_type="n2-highmem-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        )
    )

    wait_for_bhr_ping >> bhr_collection
