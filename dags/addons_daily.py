from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
#from utils.constants import DS_WEEKLY

default_args = {
    'owner': 'bmiroglio@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2019, 5, 15),
    'email': ['telemetry-alerts@mozilla.com', 'bmiroglio@mozilla.com',
    'smelancon@mozilla.com','bwright@mozilla.com','dthorn@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('addons_daily', default_args=default_args, schedule_interval='@daily')

wait_for_main_summary = ExternalTaskSensor(
    task_id='wait_for_main_summary',
    external_dag_id='main_summary',
    external_task_id='main_summary',
    execution_delta=timedelta(hours=-1), # main_summary waits one hour, execution date is beginning of the week
    dag=dag)

usage_report = EMRSparkOperator(
    task_id="addons_daily",
    job_name="Addons Daily",
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    owner="bmiroglio@mozilla.com",
    email=['telemetry-alerts@mozilla.com', 'bmiroglio@mozilla.com',
    'smelancon@mozilla.com','bwright@mozilla.com','dthorn@mozilla.com'],
    env={"date": "{{ ds_nodash }}",
         "deploy_environment": "{{ task.__class__.deploy_environment }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addons_daily.sh",
    dag=dag)

usage_report.set_upstream(wait_for_main_summary)
