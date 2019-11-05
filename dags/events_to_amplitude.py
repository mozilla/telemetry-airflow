from airflow import DAG
from datetime import datetime, timedelta

from utils.constants import DS_WEEKLY
from utils.dataproc import moz_dataproc_scriptrunner
from utils.deploy import get_artifact_url
from utils.status import register_status
from utils.tbv import tbv_envvar

from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.hooks.aws_hook import AwsHook

FOCUS_ANDROID_INSTANCES = 10
DEVTOOLS_INSTANCES = 10
DEVTOOLS_PRERELEASE_INSTANCES = 20
ROCKET_ANDROID_INSTANCES = 5
FENNEC_IOS_INSTANCES = 10
FIRE_TV_INSTANCES = 10
VCPUS_PER_INSTANCE = 16

# TODO - change this to 'prod' before merging
environment = "dev"
#environment = "prod"

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


default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    # TODO - change start date
    #'start_date': datetime(2018, 11, 20),
    'start_date': datetime(2019, 11, 20),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com', 'hwoo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'bootstrap_args': ['--metrics-provider', 'datadog'],
}

dag = DAG('events_to_amplitude', default_args=default_args, schedule_interval='0 1 * * *')

cluster_name = 'events-to-amplitude-dataproc-cluster'

# AWS credentials required to read aws dev s3://telemetry-airflow for amplitude keys
# or move that bucket to gcs or aws prod/data
# depending on if the jar job also reads/writes to other s3 locations
# read from s3://net-mozaws-data-us-west-2-ops-ci-artifacts

aws_conn_id = 'aws_dev_telemetry-airflow-config-read?'
aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

gcp_conn_id = 'google_cloud_airflow_dataproc'

# Ask frank 
# what the actual jar does (looks like it exports to amplitude uri)
# but what the jar inputs / outputs are (s3? etc)
# and how should we change the jar? or how does the subdagoperttor
# that exports to amplitude work and will that replace some jar functionality

# Values taken from old dags/operators/emr_spark_operator.py
slug = "telemetry-streaming"
region = "us-west-2"
bucket = "net-mozaws-data-us-west-2-ops-ci-artifacts"
owner_slug = "mozilla"
deploy_tag = "master"

focus_events_to_amplitude = SubDagOperator(
    task_id="focus_android_events_to_amplitude",
    dag=dag,
    subdag = moz_dataproc_scriptrunner(
        parent_dag_name=dag.dag_id,
        dag_name='focus_android_events_to_amplitude',
        default_args=default_args,
        cluster_name=cluster_name + '-focus',
        service_account='dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com',
        job_name="Focus_Android_Events_to_Amplitude",
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
        env={
            "date": "{{ ds_nodash }}",
            "max_requests": FOCUS_ANDROID_INSTANCES * VCPUS_PER_INSTANCE,
            "key_file": key_file("focus_android"),
            "artifact": get_artifact_url(slug,
                                         branch="master",
                                         region=region,
                                         bucket=bucket,
                                         owner_slug=owner_slug,
                                         deploy_tag=deploy_tag),
            "config_filename": "focus_android_events_schemas.json",
            # These keys are used for running the bash entrypoint script s3://telemetry-airflow and ops-ci-artifacts in aws data
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key
        },
        gcp_conn_id=gcp_conn_id,
        # TODO - pass the aws keys to this cluster create if this job's spark part writes to s3a://
        aws_conn_id=spark_job_aws_conn_id,
        # TODO - check if this is an appropriate number of instances,
        # TODO - also check if we need to resize the default instance type
        num_workers=FOCUS_ANDROID_INSTANCES,
        auto_delete_ttl='28800',
    )

'''
devtools_prerelease_events_to_amplitude = EMRSparkOperator(
    task_id="devtools_prerelease_events_to_amplitude",
    job_name="DevTools Prerelease Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=DEVTOOLS_PRERELEASE_INSTANCES,
    email=['ssuh@mozilla.com', 'telemetry-alerts@mozilla.com'],
    owner='ssuh@mozilla.com',
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": DEVTOOLS_PRERELEASE_INSTANCES * VCPUS_PER_INSTANCE,
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
    email=['akomar@mozilla.com', 'telemetry-alerts@mozilla.com'],
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

register_status(fennec_ios_events_to_amplitude, "Firefox-iOS Amplitude events",
                "Daily job sending Firefox iOS events to Amplitude.")

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

fire_tv_events_to_amplitude = EMRSparkOperator(
    owner='frank@mozilla.com',
    task_id="fire_tv_events_to_amplitude",
    job_name="Fire TV Events to Amplitude",
    execution_timeout=timedelta(hours=8),
    instance_count=FIRE_TV_INSTANCES,
    email=['frank@mozilla.com'],
    env={
        "date": "{{ ds_nodash }}",
        "max_requests": FIRE_TV_INSTANCES * VCPUS_PER_INSTANCE,
        "key_file": key_file("fire_tv"),
        "artifact": get_artifact_url(slug, branch="master"),
        "config_filename": "fire_tv_events_schemas.json",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/events_to_amplitude.sh",
    dag=dag
)
'''
