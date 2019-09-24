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
    'start_date': datetime(2019, 9, 22),
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
cluster_name = 'fx-usage-report-dataproc-cluster'
gcp_conn_id = 'google_cloud_airflow_dataproc'

# AWS credentials to read/write from output bucket
aws_conn_id = 'aws_prod_fx_usage_report'
aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

# TODO - change this back to prod bucket by removing -hwoo
output_bucket = 'net-mozaws-prod-us-west-2-data-public-hwoo'

usage_report = SubDagOperator(
    task_id="fx_usage_report",
    dag=dag,
    # TODO - do we want to modify this to use pyspark runner? env aws keys dont show up on log anymore
    subdag = moz_dataproc_scriptrunner(
        parent_dag_name=dag.dag_id,
        dag_name='fx_usage_report',
        default_args=default_args,
        cluster_name=cluster_name,
        job_name="Fx_Usage_Report",
        # TODO - this will only work after this PR merges. Until then, use a gcs path to temp-hwoo-removemelater
        #uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/fx_usage_report.sh",
        # TODO - rm me after job works
        uri='gs://temp-hwoo-removemelater/fx_usage_report.sh',
        env={"date": DS_WEEKLY,
             "bucket": output_bucket,
             "PYTHONPATH": "/usr/lib/spark/python/lib/pyspark.zip",
             # TODO - add this back before merging
             #"deploy_environment": "{{ task.__class__.deploy_environment }}",
             "deploy_environment": "dev",
             # These env variables are needed in addition to the s3a configs, since some code uses boto to list bucket objects
             "AWS_ACCESS_KEY_ID": aws_access_key,
             "AWS_SECRET_ACCESS_KEY": aws_secret_key
        },
        gcp_conn_id=gcp_conn_id,
        # This should is used to set the s3a configs for read/write to s3 for non boto calls
        aws_conn_id=aws_conn_id,
        num_workers=10,
        worker_machine_type='n1-standard-16',
        image_version='1.3',
        # TODO - add circleci piece for custom init script either in this repo or in fx usage report
        init_actions_uris=['gs://moz-fx-data-prod-airflow-dataproc-artifacts/custom_bootstrap/fx_usage_init.sh'],
    )
)

# TODO - add back
#usage_report.set_upstream(wait_for_main_summary)
