from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'relud@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 18),
    'email': ['telemetry-alerts@mozilla.com', 'relud@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('deletion', default_args=default_args, schedule_interval='@daily')

client_count_view = EMRSparkOperator(
    task_id="deletion_view",
    job_name="Deletion View",
    execution_timeout=timedelta(hours=10),
    owner="relud@mozilla.com",
    instance_count=20,
    env = {"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/deletion_view.sh",
    dag=dag)
