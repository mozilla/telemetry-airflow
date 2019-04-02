from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from .operators.emr_spark_operator import EMRSparkOperator
from .utils.constants import DS_WEEKLY

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2018, 11, 25),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('fx_usage_report', default_args=default_args, schedule_interval='@weekly')

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

usage_report.set_upstream(wait_for_main_summary)
