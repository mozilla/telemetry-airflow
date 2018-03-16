from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 6, 27),
    'email': ['telemetry-alerts@mozilla.com', 'mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('crash_aggregates', default_args=default_args, schedule_interval='@daily')

crash_aggregate_view = EMRSparkOperator(
    task_id = "crash_aggregate_view",
    job_name = "Crash Aggregate View",
    instance_count = 9,
    execution_timeout=timedelta(hours=4),
    env = tbv_envvar("com.mozilla.telemetry.views.CrashAggregateView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag = dag)
