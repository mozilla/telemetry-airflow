from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'relud@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 1),
    'email': ['telemetry-alerts@mozilla.com', 'relud@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('client_count_daily', default_args=default_args, schedule_interval='@daily')

client_count_view = EMRSparkOperator(
    task_id="client_count_daily_view",
    job_name="Client Count Daily View",
    execution_timeout=timedelta(hours=10),
    owner="relud@mozilla.com",
    instance_count=2,
    env = {"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/client_count_daily_view.sh",
    dag=dag)
