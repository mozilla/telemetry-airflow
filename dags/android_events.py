from airflow import DAG
from datetime import datetime, timedelta
from .operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 29),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('android_events', default_args=default_args, schedule_interval='@daily')

android_events = EMRSparkOperator(
    task_id="android_events",
    job_name="Update android events",
    execution_timeout=timedelta(hours=4),
    instance_count=5,
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    env={"date": "{{ ds_nodash }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/android-events.ipynb",
    output_visibility="public",
    dag=dag)
