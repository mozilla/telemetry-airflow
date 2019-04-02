from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2018, 12, 23),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('prerelease_telemetry_aggregates', default_args=default_args, schedule_interval='@daily')

prerelease_telemetry_aggregate_view = EMRSparkOperator(
    task_id = "prerelease_telemetry_aggregate_view",
    job_name = "Prerelease Telemetry Aggregate View",
    instance_count = 10,
    execution_timeout=timedelta(hours=12),
    env = {
      "date": "{{ ds_nodash }}",
      "channels": "nightly,aurora,beta"
    },
    uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/run_telemetry_aggregator.sh",
    dag = dag)


