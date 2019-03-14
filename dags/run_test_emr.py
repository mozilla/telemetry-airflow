from airflow import DAG
from datetime import timedelta, datetime
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 13),
    'email': ['hwoo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

dag_daily = DAG('run_test_emr',
                default_args=default_args,
                schedule_interval='@daily')

probe_scraper = EMRSparkOperator(
    task_id="run_test_emr",
    job_name="Test EMR",
    execution_timeout=timedelta(hours=4),
    instance_count=1,
    email=['hwoo@mozilla.com'],
    env={},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/run_test_emr.sh",
    output_visibility="public",
dag=dag_daily)
