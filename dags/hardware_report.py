from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta

from utils.constants import DS_WEEKLY
from utils.dataproc import moz_dataproc_pyspark_runner

"""

The following WTMO connections are needed in order for this job to run:
conn - aws_dev_telemetry_public_analysis_2_rw
"""

default_args = {
    "owner": "frank@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 20),
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com",
              "firefox-hardware-report-feedback@mozilla.com", "akomar@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# run every Monday to maintain compatibility with legacy ATMO schedule
dag = DAG("hardware_report", default_args=default_args, schedule_interval="0 2 * * MON")

# Unsalted cluster name so subsequent runs fail if the cluster name exists
cluster_name = 'hardware-report-dataproc-cluster'

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = 'google_cloud_airflow_dataproc'
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

# Required to write json output back to s3://telemetry-public-analysis-2/game-hardware-survey/data/hwsurvey-weekly-2019
write_aws_conn_id='aws_dev_telemetry_public_analysis_2_rw'
aws_access_key, aws_secret_key, session = AwsHook(write_aws_conn_id).get_credentials()

crash_report_parquet = SubDagOperator(
    task_id="hardware_report",
    dag=dag,
    subdag = moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="hardware_report",
        default_args=default_args,
        cluster_name=cluster_name,
        job_name="Firefox_Hardware_Report",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/hardware_report.py",
        init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
        additional_metadata={'PIP_PACKAGES': "google-cloud-bigquery==1.21.0 python_moztelemetry==0.10.2 boto3==1.9.87 click==6.7 click_datetime==0.2 requests-toolbelt==0.8.0 requests==2.20.1 typing==3.6.4"},
        additional_properties={"spark:spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest.jar",
                               "spark-env:AWS_ACCESS_KEY_ID": aws_access_key,
                               "spark-env:AWS_SECRET_ACCESS_KEY": aws_secret_key},
        py_args=[
            "--start_date", DS_WEEKLY,
            "--bucket", "{{ task.__class__.public_output_bucket }}",
            "--spark-provider", "dataproc",
        ],
        idle_delete_ttl='14400',
        num_workers=15,
        worker_machine_type='n1-standard-4',
        gcp_conn_id=gcp_conn_id)
)