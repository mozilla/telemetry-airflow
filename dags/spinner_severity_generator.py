from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('tab_spinner_severity', default_args=default_args, schedule_interval='@daily')

update_tab_spinner_severity = EMRSparkOperator(
    task_id="update_tab_spinner_severity",
    job_name="Tab Spinner Severity Job",
    execution_timeout=timedelta(hours=12),
    instance_count=12,
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/run-severity-generator.sh",
    dag=dag
)

