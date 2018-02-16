from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar


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

sync_view = EMRSparkOperator(
    task_id="sync_view",
    job_name="Sync Pings View",
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/sync_view.sh",
    dag=dag)

sync_events_view = EMRSparkOperator(
    task_id="sync_events_view",
    job_name="Sync Events View",
    execution_timeout=timedelta(hours=10),
    instance_count=1,
    email=['ssuh@mozilla.com'],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/sync_events_view.sh",
    dag=dag)

sync_flat_view = EMRSparkOperator(
    task_id="sync_flat_view",
    job_name="Flattened Sync Pings View",
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env = tbv_envvar("com.mozilla.telemetry.views.SyncFlatView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

sync_bookmark_validation = EMRSparkOperator(
    task_id="sync_bookmark_validation",
    job_name="Sync Bookmark Validation",
    execution_timeout=timedelta(hours=2),
    instance_count=1,
    email=["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    env=mozetl_envvar("sync_bookmark_validation", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)


sync_bookmark_validation.set_upstream(sync_view)
