from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 7, 1),
    'email': ['telemetry-alerts@mozilla.com', 'mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('addons', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="addons",
                      job_name="Addons View",
                      execution_timeout=timedelta(hours=4),
                      release_label="emr-5.0.0",
                      instance_count=10,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addons_view.sh",
                      dag=dag)
