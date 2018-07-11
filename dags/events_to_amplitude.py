from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.mozetl import mozetl_envvar
from utils.deploy import get_artifact_url

FOCUS_ANDROID_INSTANCES = 10
SAVANT_INSTANCES = 10
VCPUS_PER_INSTANCE = 16

environment = "{{ task.__class__.deploy_environment }}"
key_file = "s3://telemetry-airflow/config/amplitude/{}/apiKey".format(environment)

slug = "{{ task.__class__.telemetry_streaming_slug }}"
tag = "v1.0.1"
url = get_artifact_url(slug, tag=tag)

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



focus_events_to_amplitude = EMRSparkOperator(
    task_id="focus_android_events_to_amplitude",
    job_name="Focus Android Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=FOCUS_ANDROID_INSTANCES,
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": FOCUS_ANDROID_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file,
        "artifact": url,
        "config_filename": "focus_android_events_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag)

savant_events_to_amplitude = EMRSparkOperator(
    task_id="desktop_savant_events_to_amplitude",
    job_name="Desktop Events for Savant Study to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=SAVANT_INSTANCES,
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": SAVANT_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file,
        "artifact": url,
        "config_filename": "desktop_savant_events_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag)
