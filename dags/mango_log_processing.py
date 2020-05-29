from datetime import datetime, timedelta

from airflow import DAG
from operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor


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
