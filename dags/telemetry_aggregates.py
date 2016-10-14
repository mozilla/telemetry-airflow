from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'rvitillo@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2016, 6, 29),
    'email': ['telemetry-alerts@mozilla.com', 'rvitillo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('telemetry_aggregates', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id = "telemetry_aggregate_view",
                      job_name = "Telemetry Aggregate View",
                      instance_count = 6,
                      execution_timeout=timedelta(hours=10),
                      env = {"date": "{{ ds_nodash }}"},
                      uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_aggregator.py",
                      dag = dag)
