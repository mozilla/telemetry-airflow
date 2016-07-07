from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'rvitillo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 7, 7),
    'email': ['telemetry-alerts@mozilla.com', 'rvitillo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('longitudinal', default_args=default_args, schedule_interval='@weekly')

t0 = EMRSparkOperator(task_id="longitudinal",
                      job_name="Longitudinal View",
                      execution_timeout=timedelta(hours=10),
                      instance_count=30,
                      env = {"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.airflow_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/longitudinal.sh",
                      dag=dag)
