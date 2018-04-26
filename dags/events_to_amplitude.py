from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar

FOCUS_ANDROID_INSTANCES = 10
VCPUS_PER_INSTANCE = 16

environment = "{{ task.__class__.deploy_environment }}"
key_file = "s3://telemetry-airflow/config/amplitude/{}/apiKey".format(environment)

bucket = "{{ task.__class__.artifacts_bucket }}"
mozilla_slug = "{{ test.__class__.mozilla_slug }}"
slug = "{{ task.__class__.telemetry_streaming_slug }}"
deploy_tag = "{{ task.__class__.deploy_tag }}"
url = "https://s3-us-west-2.amazonaws.com/{bucket}/{mozilla_slug}/{slug}/{deploy_tag}/{slug}.jar".format(
    bucket=bucket, mozilla_slug=mozilla_slug, slug=slug, deploy_tag=deploy_tag)

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('events_to_amplitude', default_args=default_args, schedule_interval='0 1 * * *')

events_to_amplitude = EMRSparkOperator(
    task_id="focus_android_events_to_amplitude",
    job_name="Focus Android Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=FOCUS_ANDROID_INSTANCES,
    release_label="emr-5.13.0",
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": FOCUS_ANDROID_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file,
        "artifact": url
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/focus_android_events_to_amplitude.sh",
    dag=dag)
