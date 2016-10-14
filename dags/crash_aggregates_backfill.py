from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'mdoglio@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 9, 20),
    'email': ['telemetry-alerts@mozilla.com', 'mdoglio@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('crash_aggregates_backfill', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id = "crash_aggregates_view_backfill",
                      job_name = "Crash Aggregates View Backfill",
                      release_label="emr-5.0.0",
                      instance_count = 20,
                      execution_timeout=timedelta(hours=4),
                      env = {"date": "{{ ds_nodash }}", "bucket": "telemetry-test-bucket"},
                      uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/crash_aggregate_view.sh",
                      dag = dag)
