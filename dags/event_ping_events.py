from airflow import DAG
from datetime import datetime, timedelta
from .operators.emr_spark_operator import EMRSparkOperator
from .utils.deploy import get_artifact_url
from .utils.tbv import tbv_envvar


slug = "{{ task.__class__.telemetry_streaming_slug }}"
url = get_artifact_url(slug)

default_args = {
    'owner': 'ssuh@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['telemetry-alerts@mozilla.com', 'ssuh@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('event_ping_events', default_args=default_args, schedule_interval='0 1 * * *')

event_ping_events = EMRSparkOperator(
    task_id="event_ping_events",
    job_name="Event Ping Events Dataset",
    execution_timeout=timedelta(hours=8),
    instance_count=5,
    env=tbv_envvar("com.mozilla.telemetry.streaming.EventPingEvents", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "outputPath": "s3://{{ task.__class__.private_output_bucket }}/"
    }, artifact_url=url),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)
