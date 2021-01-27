from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG('probe_scraper',
         default_args=default_args,
         schedule_interval='0 0 * * 1-5') as dag:

    aws_conn_id='aws_prod_probe_scraper'
    aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

    # Built from repo https://github.com/mozilla/probe-scraper
    probe_scraper_image='gcr.io/moz-fx-data-airflow-prod-88e0/probe-scraper:latest'
    probe_scraper_args = [
        'python3', '-m', 'probe_scraper.runner',
        '--out-dir', '/app/probe_data',
        '--cache-dir', '/app/probe_cache',
        '--output-bucket', 'net-mozaws-prod-us-west-2-data-pitmo',
        '--cache-bucket', 'telemetry-airflow-cache',
        '--env', 'prod'
    ]

    # Cluster autoscaling works on pod resource requests, instead of usage
    resources = {'request_memory':'13312Mi', 'request_cpu': None,
                 'limit_memory':'30720Mi', 'limit_cpu': None, 'limit_gpu': None}

    probe_scraper = GKEPodOperator(
        task_id="probe_scraper",
        name='probe-scraper',
        # Needed to scale the highmem pool from 0 -> 1
        resources=resources,
        # This python job requires 13 GB of memory, thus the highmem node pool
        node_selectors={"nodepool" : "highmem"},
        # Due to the nature of the container run, we set get_logs to False,
        # To avoid urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes read)' errors
        # Where the pod continues to run, but airflow loses its connection and sets the status to Failed
        get_logs=False,
        # Give additional time since we will likely always scale up when running this job
        startup_timeout_seconds=360,
        image=probe_scraper_image,
        arguments=probe_scraper_args,
        email=['telemetry-client-dev@mozilla.com', 'aplacitelli@mozilla.com', 'frank@mozilla.com', 'hwoo@mozilla.com'],
        env_vars={
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key
        },
        dag=dag)

    schema_generator = GKEPodOperator(
        email=['frank@mozilla.com'],
        task_id='mozilla_schema_generator',
        name='schema-generator-1',
        image='mozilla/mozilla-schema-generator:latest',
        env_vars={
            "MPS_SSH_KEY_BASE64": "{{ var.value.mozilla_pipeline_schemas_secret_git_sshkey_b64 }}",
            "MPS_REPO_URL": "git@github.com:mozilla-services/mozilla-pipeline-schemas.git",
            "MPS_BRANCH_SOURCE": "master",
            "MPS_BRANCH_PUBLISH": "generated-schemas",
        },
        dag=dag)

    schema_generator.set_upstream(probe_scraper)

    probe_expiry_alerts = GKEPodOperator(
        task_id="probe-expiry-alerts",
        name="probe-expiry-alerts",
        image=probe_scraper_image,
        arguments=[
            "python3", "-m", "probe_scraper.probe_expiry_alert",
            "--date", "{{ ds }}",
            "--bugzilla-api-key", "{{ var.value.bugzilla_probe_expiry_bot_api_key }}"
        ],
        email=["bewu@mozilla.com"],
        env_vars={
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key
        },
        dag=dag)

    probe_expiry_alerts.set_upstream(probe_scraper)
