import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from operators.task_sensor import ExternalTaskCompletedSensor
from airflow.operators.subdag_operator import SubDagOperator

from utils.dataproc import moz_dataproc_pyspark_runner, get_dataproc_parameters

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "telemetry-alerts@mozilla.com",
        "bewu@mozilla.com",
        "mmynttinen@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=20),
}

PIP_PACKAGES = [
    "python_moztelemetry",
    "git+https://github.com/FirefoxGraphics/telemetry.git#egg=pkg&subdirectory=analyses/bigquery_shim",
    "boto3==1.16.20",
    "six==1.15.0",
]

S3_BUCKET = "telemetry-public-analysis-2"
S3_PREFIX = "gfx/telemetry-data/"

with DAG(
        "graphics_telemetry",
        default_args=default_args,
        schedule_interval="0 3 * * *",
) as dag:
    # Jobs read from/write to s3://telemetry-public-analysis-2/gfx/telemetry-data/
    write_aws_conn_id = 'aws_dev_telemetry_public_analysis_2_rw'
    aws_access_key, aws_secret_key, _ = AwsHook(write_aws_conn_id).get_credentials()

    wait_for_main_ping = ExternalTaskCompletedSensor(
        task_id="wait_for_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(hours=2),
        check_existence=True,
        mode="reschedule",
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
            image_version="1.5",
            dag_name="graphics_trends",
            default_args=default_args,
            cluster_name="graphics-trends-{{ ds }}",
            job_name="graphics-trends",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/graphics/graphics_telemetry_trends.py",
            init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
            additional_metadata={'PIP_PACKAGES': " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": aws_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": aws_secret_key
            },
            py_args=[
                "--s3-bucket", S3_BUCKET,
                "--s3-prefix", S3_PREFIX,
                "--weekly-fraction", "0.003",
            ],
            idle_delete_ttl="14400",
            num_workers=2,
            worker_machine_type="n1-standard-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        )
    )

    graphics_dashboard = SubDagOperator(
        task_id="graphics_dashboard",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5",
            dag_name="graphics_dashboard",
            default_args=default_args,
            cluster_name="graphics-dashboard-{{ ds }}",
            job_name="graphics-dashboard",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/graphics/graphics_telemetry_dashboard.py",
            init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
            additional_metadata={'PIP_PACKAGES': " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": aws_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": aws_secret_key
            },
            py_args=[
                "--output-bucket", "telemetry-public-analysis-2",
                "--output-prefix", "gfx/telemetry-data/",
                "--release-fraction", "0.003",
            ],
            idle_delete_ttl="14400",
            num_workers=2,
            worker_machine_type="n1-highmem-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        )
    )

    wait_for_main_ping >> graphics_trends
    wait_for_main_ping >> graphics_dashboard
