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
    'start_date': datetime(2019, 9, 25),
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

cluster_name = 'fx-usage-report-dataproc-cluster'
gcp_conn_id = 'google_cloud_airflow_dataproc'

# AWS credentials to read/write from output bucket
aws_conn_id = 'aws_prod_fx_usage_report'
aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

output_bucket = 'net-mozaws-prod-us-west-2-data-public-hwoo'

usage_report = SubDagOperator(
    task_id="fx_usage_report",
    dag=dag,
    subdag = moz_dataproc_scriptrunner(
        parent_dag_name=dag.dag_id,
        dag_name='fx_usage_report',
        default_args=default_args,
        cluster_name=cluster_name,
        job_name="Fx_Usage_Report",
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/fx_usage_report.sh",
        env={"date": DS_WEEKLY,
             "AWS_ACCESS_KEY_ID": aws_access_key,
             "AWS_SECRET_ACCESS_KEY": aws_secret_key,
             "bucket": output_bucket,
             "deploy_environment": "{{ task.__class__.deploy_environment }}"
        },
        gcp_conn_id=gcp_conn_id,
        aws_conn_id=aws_conn_id,
        num_workers=8,
    )
)

# TODO - add back
#usage_report.set_upstream(wait_for_main_summary)
