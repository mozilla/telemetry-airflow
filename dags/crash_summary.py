from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.tbv import tbv_envvar

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

crash_summary_view = EMRSparkOperator(
    task_id="crash_summary_view",
    job_name="Crash Summary View",
    instance_count=20,
    execution_timeout=timedelta(hours=4),
    env = tbv_envvar("com.mozilla.telemetry.views.CrashSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "outputBucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)
