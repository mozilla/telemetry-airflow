from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta

from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from utils.constants import DS_WEEKLY
from utils.dataproc import moz_dataproc_scriptrunner

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2019, 9, 16),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

# TODO - change back to weekly
#dag = DAG('fx_usage_report', default_args=default_args, schedule_interval='@weekly')
dag = DAG('fx_usage_report', default_args=default_args, schedule_interval='@daily')

# TODO - add back
"""
wait_for_main_summary = ExternalTaskSensor(
    task_id='wait_for_main_summary',
    external_dag_id='main_summary',
    external_task_id='main_summary',
    execution_delta=timedelta(days=-7, hours=-1), # main_summary waits one hour, execution date is beginning of the week
    dag=dag)

"""

# TODO - name this something good
cluster_name = 'fx-usage-report-dataproc-cluster-1'
gcp_conn_id = 'google_cloud_airflow_dataproc'

# AWS credentials to read/write from output bucket
aws_conn_id = 'aws_prod_fx_usage_report'

# TODO - change this back to prod bucket by removing -hwoo
output_bucket = 'net-mozaws-prod-us-west-2-data-public-hwoo'

usage_report = SubDagOperator(
    task_id="fx_usage_report",
    dag=dag,
    # TODO - do we want to modify this to use pyspark runner bc this one will output aws creds on log line
    subdag = moz_dataproc_scriptrunner(
        parent_dag_name=dag.dag_id,
        dag_name='fx_usage_report',
        default_args=default_args,
        cluster_name=cluster_name,
        job_name="Fx_Usage_Report",
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/fx_usage_report.sh",
        env={"date": DS_WEEKLY,
             "bucket": output_bucket,
             "PYTHONPATH": "/usr/lib/spark/python/lib/pyspark.zip",
             "deploy_environment": "{{ task.__class__.deploy_environment }}"
        },
        gcp_conn_id=gcp_conn_id,
        # This should be sufficient to set the s3a configs for read/write to s3
        aws_conn_id=aws_conn_id,
        num_workers=8,
        # TODO - is this the right version since we want pyspark 2.2.2
        image_version='1.3',
        # TODO - add circleci piece for custom init script either in this repo or in fx usage report
        init_actions_uris=['gs://moz-fx-data-prod-airflow-dataproc-artifacts/custom_bootstrap/fx_usage_init.sh'],
    )
)

# TODO - add back
#usage_report.set_upstream(wait_for_main_summary)
