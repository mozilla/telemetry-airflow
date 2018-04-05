from datetime import datetime, timedelta

from airflow import DAG
from operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor


DEFAULT_ARGS = {
    'owner': 'jthomas@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 2),
    'email': ['jthomas@mozilla.com', 'dataops+alerts@mozilla.com'],
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

blp_logs.set_downstream(blp_job_sensor)

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
