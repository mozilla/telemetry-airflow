from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 3, 12),
    'end_date': datetime(2016, 6, 9),
    'email': ['mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('backfill_main_summary_v3', default_args=default_args, schedule_interval='@daily', max_active_runs=5)

t1 = EMRSparkOperator(task_id="backfill_main_summary",
                      job_name="Backfill Main Summary View",
                      execution_timeout=timedelta(hours=10),
                      instance_count=10,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_summary_view.sh",
                      dag=dag)
