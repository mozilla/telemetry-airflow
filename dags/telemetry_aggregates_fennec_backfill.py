from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': True,
    'start_date': datetime(2016, 12, 15),
    'email': ['frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('telemetry_aggregates_fennec_backfill', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id = "telemetry_aggregate_fennec_backfill",
                      job_name = "Telemetry Aggregate Fennec Backfill",
                      instance_count = 7,
                      execution_timeout=timedelta(hours=5),
                      env = {"date": "{{ ds_nodash }}"},
                      uri = "https://raw.githubusercontent.com/fbertsch/python_mozaggregator/fennec_backfill/telemetry_aggregator.py",
                      dag = dag)
