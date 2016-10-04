from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from airflow.operators import BashOperator

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2015, 11, 3),
    'email': ['telemetry-alerts@mozilla.com', 'mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('main_summary', default_args=default_args, schedule_interval='@daily')

# Make sure all the data for the given day has arrived before running.
t0 = BashOperator(task_id="delayed_start",
                  bash_command="sleep 3600",
                  dag=dag)

t1 = EMRSparkOperator(task_id="main_summary",
                      job_name="Main Summary View",
                      execution_timeout=timedelta(hours=10),
                      release_label="emr-5.0.0",
                      instance_count=10,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_summary_view.sh",
                      dag=dag)

# Wait a little while after midnight to start for a given day.
t1.set_upstream(t0)
