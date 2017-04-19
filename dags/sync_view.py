from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'markh@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 7, 12),
    'email': ['telemetry-alerts@mozilla.com', 'markh@mozilla.com', 'tchiovoloni@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('sync_view', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="sync_view",
                      job_name="Sync Pings View",
                      execution_timeout=timedelta(hours=10),
                      instance_count=10,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/sync_view.sh",
                      dag=dag)

t1 = EMRSparkOperator(task_id="sync_events_view",
                      job_name="Sync Events View",
                      execution_timeout=timedelta(hours=10),
                      instance_count=1,
                      email=['ssuh@mozilla.com'],
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/sync_events_view.sh",
                      dag=dag)
