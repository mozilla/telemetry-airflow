from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'mdoglio@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 30),
    'email': ['telemetry-alerts@mozilla.com', 'mdoglio@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('crash_summary', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="crash_summary_view",
                      job_name="Crash Summary View",
                      instance_count=20,
                      execution_timeout=timedelta(hours=4),
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/crash_summary_view.sh",
                      dag=dag)
