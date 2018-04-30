from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.mozetl import mozetl_envvar
from utils.deploy import get_artifact_url

FOCUS_ANDROID_INSTANCES = 10
VCPUS_PER_INSTANCE = 16

environment = "{{ task.__class__.deploy_environment }}"
key_file = "s3://telemetry-airflow/config/amplitude/{}/apiKey".format(environment)
config_file = "focus_android_events_schemas.json"

slug = "{{ task.__class__.telemetry_streaming_slug }}"
url = get_artifact_url(slug)

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
        "artifact": url,
        "config_filename": config_file
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag)
