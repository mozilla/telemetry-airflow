from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'robhudson@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2018, 9, 21),
    'email': ['telemetry-alerts@mozilla.com', 'robhudson@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('telemetry_aggregates_parquet', default_args=default_args, schedule_interval='@daily')

telemetry_aggregate_parquet_view = EMRSparkOperator(
    task_id="telemetry_aggregate_parquet_view",
    job_name="Telemetry Aggregate Parquet View",
    instance_count=5,
    execution_timeout=timedelta(hours=12),
    env={
      "date": "{{ ds_nodash }}",
      "channels": "nightly",
      "bucket": "{{ task.__class__.private_output_bucket }}",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/run_telemetry_aggregator_parquet.sh",
    dag=dag)
