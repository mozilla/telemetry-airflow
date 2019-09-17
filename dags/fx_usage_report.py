from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta

from utils.constants import DS_WEEKLY

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

usage_report = EMRSparkOperator(
    task_id="fx_usage_report",
    job_name="Fx Usage Report",
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "shong@mozilla.com"],
    env={"date": DS_WEEKLY,
         "bucket": "net-mozaws-prod-us-west-2-data-public",
         "deploy_environment": "{{ task.__class__.deploy_environment }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/fx_usage_report.sh",
    dag=dag)
"""

cluster_name = 'fx-usage-report-dataproc-cluster'
gcp_conn_id = 'google_cloud_airflow_dataproc'

# AWS credentials to ???
aws_conn_id = 'aws_prod_fx_usage_report'

usage_report = SubDagOperator(
    task_id="fx_usage_report",
    dag=dag,
    subdag = moz_dataproc_scriptrunner(
        job_name="Fx_Usage_Report",
        dag_name='run_fx_usage_report_script',
        default_args=default_args,
        cluster_name=cluster_name,
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/fx_usage_report.sh",
        env={"date": DS_WEEKLY,
             "bucket": "net-mozaws-prod-us-west-2-data-public",
             "deploy_environment": "{{ task.__class__.deploy_environment }}"
        },
        gcp_conn_id=gcp_conn_id,
        aws_conn_id=aws_conn_id,
        num_workers=8,
        owner="frank@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "shong@mozilla.com"],
    )
)

# TODO - add back
#usage_report.set_upstream(wait_for_main_summary)
