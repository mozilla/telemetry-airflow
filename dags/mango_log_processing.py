from datetime import datetime, timedelta

from airflow import DAG
from operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

from utils.status import register_status


DEFAULT_ARGS = {
    'owner': 'jthomas@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['jthomas@mozilla.com', 'hwoo@mozilla.com', 'dataops+alerts@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

BLP_STEPS = [
    {
        # BLP nginx
        "Name": "blocklists.settings.services.mozilla.com",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "blocklists.settings.services.mozilla.com",
                "--bucket", "net-mozaws-prod-kintoblocklist-blocklist-kbprod1",
                "--date", "{{ ds }}"
            ]
        }
    }
]

AMO_STEPS = [
    {
        # AMO nginx - stats
        "Name": "addons.mozilla.org",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "addons.mozilla.org",
                "--bucket", "amo-metrics-logs-prod",
                "--date", "{{ ds }}",
            ]
        }
    },
    {
        # VAMO nginx logs - stats
        "Name": "versioncheck.addons.mozilla.org",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "versioncheck.addons.mozilla.org",
                "--bucket", "amo-metrics-logs-prod",
                "--reducers", "120",
                "--date", "{{ ds }}"
            ]
        }
    }
]

AMO_DEV_STAGE_STEPS = [
    {
        # AMO DEV nginx - stats
        "Name": "addons-dev.allizom.org",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "addons-dev.allizom.org",
                "--bucket", "amo-metrics-logs-dev",
                "--date", "{{ ds }}",
            ]
        }
    },
    {
        # VAMO DEV nginx logs - stats
        "Name": "versioncheck-dev.allizom.org",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "versioncheck-dev.allizom.org",
                "--bucket", "amo-metrics-logs-dev",
                "--date", "{{ ds }}"
            ]
        }
    },
    {
        # AMO STAGE nginx - stats
        "Name": "addons.allizom.org",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "addons.allizom.org",
                "--bucket", "amo-metrics-logs-stage",
                "--date", "{{ ds }}",
            ]
        }
    },
    {
        # VAMO STAGE nginx logs - stats
        "Name": "versioncheck.allizom.org",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/local/bin/processlogs",
                "--domain", "versioncheck.allizom.org",
                "--bucket", "amo-metrics-logs-stage",
                "--date", "{{ ds }}"
            ]
        }
    }
]

blp_dag = DAG(
    'mango_log_processing_adi',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval='0 3 * * *'
)

blp_logs = EmrCreateJobFlowOperator(
    task_id='blp_create_job_flow',
    job_flow_overrides={'Steps': BLP_STEPS},
    aws_conn_id='aws_data_iam',
    emr_conn_id='emr_data_iam_mango',
    dag=blp_dag
)

blp_job_sensor = EmrJobFlowSensor(
    task_id='blp_check_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('blp_create_job_flow', key='return_value') }}",
    aws_conn_id='aws_data_iam',
    dag=blp_dag,
    on_retry_callback=lambda context: blp_dag.clear(
        start_date=context['execution_date'],
        end_date=context['execution_date']),
)

gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

gcstj_object_conditions = {
    'includePrefixes':  'blpadi/{{ ds }}'
}

gcstj_transfer_options = {
    'deleteObjectsUniqueInSink': True
}

bq_args = [
    'bq',
    '--location=US',
    'load',
    '--source_format=CSV',
    '--skip_leading_rows=0',
    '--replace',
    "--field_delimiter=\001",
    'blpadi.adi_dimensional_by_date${{ ds_nodash }}',
    'gs://moz-fx-data-derived-datasets-blpadi/blpadi/{{ ds }}/*',
]

s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
    task_id='s3_to_gcs',
    s3_bucket='net-mozaws-data-us-west-2-data-analysis',
    gcs_bucket='moz-fx-data-derived-datasets-blpadi',
    description='blpadi copy from s3 to gcs',
    aws_conn_id='aws_data_iam_blpadi',
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    object_conditions=gcstj_object_conditions,
    transfer_options=gcstj_transfer_options,
    timeout=720,
    dag=blp_dag
)

load_blpadi_to_bq = GKEPodOperator(
    task_id='bigquery_load',
    name='load-blpadi-to-bq',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=bq_args,
    dag=blp_dag
)

blp_logs.set_downstream(blp_job_sensor)
blp_job_sensor.set_downstream(s3_to_gcs)
s3_to_gcs.set_downstream(load_blpadi_to_bq)

amo_dag = DAG(
    'mango_log_processing_amo',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval='0 3 * * *'
)

amo_logs = EmrCreateJobFlowOperator(
    task_id='amo_create_job_flow',
    job_flow_overrides={'Steps': AMO_STEPS},
    aws_conn_id='aws_data_iam',
    emr_conn_id='emr_data_iam_mango',
    dag=amo_dag
)

register_status(amo_logs, 'AMO Logs', 'Mango Processed AMO Logs')

amo_job_sensor = EmrJobFlowSensor(
    task_id='amo_check_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('amo_create_job_flow', key='return_value') }}",
    aws_conn_id='aws_data_iam',
    dag=amo_dag,
    on_retry_callback=lambda context: amo_dag.clear(
        start_date=context['execution_date'],
        end_date=context['execution_date']),
)

amo_logs.set_downstream(amo_job_sensor)

# For AMO Dev and Stage Environments
amo_dev_stage_dag = DAG(
    'mango_log_processing_amo_dev_stage',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

amo_dev_stage_logs = EmrCreateJobFlowOperator(
    task_id='amo_dev_stage_create_job_flow',
    job_flow_overrides={'Steps': AMO_DEV_STAGE_STEPS},
    aws_conn_id='aws_data_iam',
    emr_conn_id='emr_data_iam_mango',
    dag=amo_dev_stage_dag
)

amo_dev_stage_job_sensor = EmrJobFlowSensor(
    task_id='amo_dev_stage_check_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('amo_dev_stage_create_job_flow', key='return_value') }}",
    aws_conn_id='aws_data_iam',
    dag=amo_dev_stage_dag,
    on_retry_callback=lambda context: amo_dev_stage_dag.clear(
        start_date=context['execution_date'],
        end_date=context['execution_date']),
)

amo_dev_stage_logs.set_downstream(amo_dev_stage_job_sensor)
