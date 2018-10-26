from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 9, 20),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('android_clients', default_args=default_args, schedule_interval='@daily')

android_clients = EMRSparkOperator(
    task_id="android_clients",
    job_name="Update android clients",
    execution_timeout=timedelta(hours=4),
    instance_count=5,
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    env={"date": "{{ ds_nodash }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/android-clients.ipynb",
    output_visibility="public",
    dag=dag)
