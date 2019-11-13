from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa

from airflow.operators.subdag_operator import SubDagOperator

from utils.dataproc import moz_dataproc_jar_runner
from utils.dataproc import moz_dataproc_scriptrunner

from utils.status import register_status


DEFAULT_ARGS = {
    'owner': 'jthomas@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 7),
    'email': ['jthomas@mozilla.com', 'hwoo@mozilla.com', 'dataops+alerts@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

blp_dag = DAG(
    'mango_log_processing_adi',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval='0 3 * * *'
)

aws_conn_id = 'aws_data_iam'
aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

blp_cluster_name = 'mango_blocklist_dataproc_cluster'

blp_logs = SubDagOperator(
    task_id='mango_blocklist_log_processing',
    dag=blp_dag,
    subdag = moz_dataproc_scriptrunner(
        parent_dag_name=blp_dag.dag_id,
        dag_name='mango_blocklist_log_processing',
        default_args=DEFAULT_ARGS,
        cluster_name=blp_cluster_name,
        service_account='dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com',
        job_name="blocklists.settings.smo_log_processing",
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/mango_blocklist_processlogs.sh",
        env={"PYTHONPATH": "/usr/lib/spark/python/lib/pyspark.zip",
             # These env variables are needed in addition to the s3a configs, since some code uses boto to delete prefixes
             "AWS_ACCESS_KEY_ID": aws_access_key,
             "AWS_SECRET_ACCESS_KEY": aws_secret_key,
             "date": "{{ ds }}",
             "bucket": "net-mozaws-prod-kintoblocklist-blocklist-kbprod1",
             "domain": "blocklists.settings.services.mozilla.com"
        },
        gcp_conn_id=gcp_conn_id,
        # This should is used to set the s3a configs for read/write to s3 for non boto calls
        aws_conn_id=aws_conn_id,
        num_preemptible_workers=40,
        worker_machine_type='n1-highmem-4',
        # TODO - check image version with lib versions? or just test
        image_version='1.3',
        # TODO add init scripts
        init_actions_uris=['gs://moz-fx-data-prod-airflow-dataproc-artifacts/bootstrap/fx_usage_init.sh'],
        # TODO - test optional components HIVE_WEBHCAT
        optional_components=['ANACONDA', 'HIVE_WEBHCAT'],
        # TODO - add additional properties for hive/hadoop/etc configs
        additional_properties = ???,
    )
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

blp_aws_conn_id = 'aws_data_iam_blpadi'

s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
    task_id='s3_to_gcs',
    s3_bucket='net-mozaws-data-us-west-2-data-analysis',
    gcs_bucket='moz-fx-data-derived-datasets-blpadi',
    description='blpadi copy from s3 to gcs',
    aws_conn_id=blp_aws_conn_id,
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    object_conditions=gcstj_object_conditions,
    transfer_options=gcstj_transfer_options,
    dag=blp_dag
)

load_blpadi_to_bq = GKEPodOperator(
    task_id='bigquery_load',
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location='us-central1-a',
    cluster_name='bq-load-gke-1',
    name='load-blpadi-to-bq',
    namespace='default',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=bq_args,
    dag=blp_dag
)

blp_logs >> s3_to_gcs >> load_blpadi_to_bq


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

amo_dag = DAG(
    'mango_log_processing_amo',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval='0 3 * * *'
)

amo_logs = EmrCreateJobFlowOperator(
    task_id='amo_create_job_flow',
    job_flow_overrides={'Steps': AMO_STEPS},
    aws_conn_id=aws_conn_id,
    emr_conn_id=emr_conn_id,
    dag=amo_dag
)

register_status(amo_logs, 'AMO Logs', 'Mango Processed AMO Logs')


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
    aws_conn_id=aws_conn_id,
    emr_conn_id=emr_conn_id,
    dag=amo_dev_stage_dag
)
