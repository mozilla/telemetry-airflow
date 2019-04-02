from airflow import DAG
from datetime import timedelta, datetime
from .operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'gfritzsche@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['telemetry-client-dev@mozilla.com', 'gfritzsche@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag_daily = DAG('probe_scraper',
                default_args=default_args,
                schedule_interval='@daily')

probe_scraper = EMRSparkOperator(
    task_id="probe_scraper",
    job_name="Probe Scraper",
    execution_timeout=timedelta(hours=4),
    instance_count=1,
    email=['telemetry-client-dev@mozilla.com', 'gfritzsche@mozilla.com', 'aplacitelli@mozilla.com', 'frank@mozilla.com'],
    env={},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/probe_scraper.sh",
    output_visibility="public",
    dag=dag_daily)
