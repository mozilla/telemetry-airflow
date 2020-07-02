from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta

from utils.dataproc import (
    moz_dataproc_pyspark_runner,
    get_dataproc_parameters,
)


"""
The following WTMO connections are needed in order for this job to run:
conn - aws_dev_telemetry_public_analysis_2_rw
"""

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 6),
    "email": ["telemetry-alerts@mozilla.com",
              "firefox-hardware-report-feedback@mozilla.com",
              "akomar@mozilla.com",
              "shong@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG("public_data_hardware_report", default_args=default_args, schedule_interval="0 1 * * MON")

# Required to write json output to s3://telemetry-public-analysis-2/public-data-report/hardware/
write_aws_conn_id='aws_dev_telemetry_public_analysis_2_rw'
aws_access_key, aws_secret_key, session = AwsHook(write_aws_conn_id).get_credentials()

# hardware_report's execution date will be {now}-7days. It will read last week's main pings,
# therefore we need to wait for yesterday's Main Ping deduplication task to finish
wait_for_main_ping = ExternalTaskSensor(
    task_id="wait_for_main_ping",
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_main_ping",
    execution_delta=timedelta(days=-6),
    check_existence=True,
    dag=dag,
)

params = get_dataproc_parameters("google_cloud_airflow_dataproc")

hardware_report = SubDagOperator(
    task_id="public_data_hardware_report",
    dag=dag,
    subdag = moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="public_data_hardware_report",
        default_args=default_args,
        cluster_name="public-data-hardware-report-{{ ds }}",
        job_name="Firefox_Public_Data_Hardware_Report-{{ ds }}",
        python_driver_code="gs://{}/jobs/moz_dataproc_runner.py".format(params.artifact_bucket),
        init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
        additional_metadata={'PIP_PACKAGES': "git+https://github.com/mozilla/firefox-public-data-report-etl.git"},
        additional_properties={"spark:spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest.jar",
                               "spark-env:AWS_ACCESS_KEY_ID": aws_access_key,
                               "spark-env:AWS_SECRET_ACCESS_KEY": aws_secret_key},
        py_args=[
            "public_data_report",
            "hardware_report",
            "--date_from", "{{ ds }}",
            "--bq_table", "moz-fx-data-shared-prod.telemetry_derived.public_data_report_hardware",
            "--temporary_gcs_bucket", params.storage_bucket,
            "--s3_bucket", "telemetry-public-analysis-2",
            "--s3_path", "public-data-report/hardware/",
        ],
        idle_delete_ttl='14400',
        num_workers=2,
        worker_machine_type='n1-standard-4',
        gcp_conn_id=params.conn_id,
        service_account=params.client_email,
        storage_bucket=params.storage_bucket,
    )
)

wait_for_main_ping >> hardware_report