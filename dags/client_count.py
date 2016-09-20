from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'rvitillo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 8, 13),
    'email': ['telemetry-alerts@mozilla.com', 'rvitillo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('client_count', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="client_count_view",
                      job_name="Client Count View",
                      execution_timeout=timedelta(hours=10),
                      release_label="emr-5.0.0",
                      instance_count=20,
                      env = {"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/client_count_view.sh",
                      dag=dag)
