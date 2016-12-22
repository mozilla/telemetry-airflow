from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from airflow.operators import BashOperator

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2015, 11, 3),
    'email': ['telemetry-alerts@mozilla.com', 'mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('main_summary', default_args=default_args, schedule_interval='@daily')

# Make sure all the data for the given day has arrived before running.
t0 = BashOperator(task_id="delayed_start",
                  bash_command="sleep 3600",
                  dag=dag)

t1 = EMRSparkOperator(task_id="main_summary",
                      job_name="Main Summary View",
                      execution_timeout=timedelta(hours=10),
                      release_label="emr-5.0.0",
                      instance_count=10,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_summary_view.sh",
                      dag=dag)

t2 = EMRSparkOperator(task_id="engagement_ratio",
                      job_name="Update Engagement Ratio",
                      execution_timeout=timedelta(hours=6),
                      instance_count=10,
                      uri="https://raw.githubusercontent.com/mozilla-services/data-pipeline/master/reports/engagement_ratio/MauDau.ipynb",
                      output_visibility="public",
                      dag=dag)

t3 = EMRSparkOperator(task_id="addons",
                      job_name="Addons View",
                      execution_timeout=timedelta(hours=4),
                      release_label="emr-5.0.0",
                      instance_count=3,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addons_view.sh",
                      dag=dag)

t4 = EMRSparkOperator(task_id="hbase_main_summary",
                      job_name="HBase Main Summary View",
                      owner="rvitillo@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "rvitillo@mozilla.com"],
                      execution_timeout=timedelta(hours=10),
                      release_label="emr-5.0.0",
                      instance_count=5,
                      env={"date": "{{ ds_nodash }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hbase_main_summary_view.sh",
                      dag=dag)

t1.set_upstream(t0)
t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)
