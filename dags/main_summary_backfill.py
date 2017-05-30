from airflow import DAG, macros
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 5, 30),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('main_summary_backfill', default_args=default_args, schedule_interval='@weekly')

t0 = EMRSparkOperator(task_id="main_summary_backfill",
                      job_name="Main Summary Backfill",
                      execution_timeout=timedelta(hours=10),
                      instance_count=20,
                      env={
                        "date": "{{ ds_nodash }}",
                        "todate": "{{ macros.ds_format(macros.ds_add(ds, 6), '%Y-%m-%d', '%Y%m%d') }}",
                        "bucket": "{{ task.__class__.private_output_bucket }}"
                      },
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_summary_backfill.sh",
                      dag=dag)
