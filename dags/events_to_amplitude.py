from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.tbv import tbv_envvar
from utils.deploy import get_artifact_url

FOCUS_ANDROID_INSTANCES = 10
DEVTOOLS_INSTANCES = 10
ROCKET_ANDROID_INSTANCES = 5
FENNEC_IOS_INSTANCES = 10
VCPUS_PER_INSTANCE = 16

environment = "{{ task.__class__.deploy_environment }}"

def key_file(project):
    return (
        "s3://telemetry-airflow/config/amplitude/{}/{}/apiKey"
        .format(environment, project)
    )

def key_path(project):
    return (
        "config/amplitude/{}/{}/apiKey"
        .format(environment, project)
    )

slug = "{{ task.__class__.telemetry_streaming_slug }}"

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 20),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'bootstrap_args': ['--metrics-provider', 'datadog'],
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
        "key_file": key_file("focus_android"),
        "artifact": get_artifact_url(slug, branch="master"),
        "config_filename": "focus_android_events_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag)

devtools_prerelease_events_to_amplitude = EMRSparkOperator(
    task_id="devtools_prerelease_events_to_amplitude",
    job_name="DevTools Prerelease Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=DEVTOOLS_INSTANCES,
    email=['ssuh@mozilla.com', 'telemetry-alerts@mozilla.com'],
    owner='ssuh@mozilla.com',
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": DEVTOOLS_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file("devtools"),
        "artifact": get_artifact_url(slug, branch="master"),
        "config_filename": "devtools_prerelease_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag)

rocket_android_events_to_amplitude = EMRSparkOperator(
    owner='nechen@mozilla.com',
    task_id="rocket_android_events_to_amplitude",
    job_name="Rocket Android Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=ROCKET_ANDROID_INSTANCES,
    email=['frank@mozilla.com', 'nechen@mozilla.com'],
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": ROCKET_ANDROID_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file("rocket_android"),
        "artifact": get_artifact_url(slug, branch="master"),
        "config_filename": "rocket_android_events_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag
)

fennec_ios_events_to_amplitude = EMRSparkOperator(
    task_id="fennec_ios_events_to_amplitude",
    job_name="Fennec iOS Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=FENNEC_IOS_INSTANCES,
    email=['mpopova@mozilla.com', 'akomar@mozilla.com', 'telemetry-alerts@mozilla.com'],
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": FENNEC_IOS_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file("fennec_ios"),
        "artifact": get_artifact_url(slug, branch="master"),
        "config_filename": "fennec_ios_events_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag
)

devtools_release_events_to_amplitude = EMRSparkOperator(
    task_id="devtools_release_events_to_amplitude",
    job_name="DevTools Release Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=DEVTOOLS_INSTANCES,
    dev_instance_count=DEVTOOLS_INSTANCES,
    email=['ssuh@mozilla.com', 'telemetry-alerts@mozilla.com'],
    owner='ssuh@mozilla.com',
    env=tbv_envvar(
        "com.mozilla.telemetry.streaming.EventsToAmplitude",
        {
            "from": "{{ ds_nodash }}",
            "to": "{{ ds_nodash }}",
            "max_parallel_requests": str(DEVTOOLS_INSTANCES * VCPUS_PER_INSTANCE),
            "config_file_path": "devtools_release_schemas.json",
            "url": "https://api.amplitude.com/httpapi",
            "sample": "0.5",
            "partition_multiplier": "5"
        },
        artifact_url=get_artifact_url(slug),
        other={
            "KEY_BUCKET": "telemetry-airflow",
            "KEY_PATH": key_path("devtools"),
            "DO_EVENTS_TO_AMPLITUDE_SETUP": "True"
        }
    ),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    start_date=datetime(2018, 12, 4),
    dag=dag)
