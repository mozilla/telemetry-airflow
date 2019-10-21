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
         schedule_interval='@daily') as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    aws_conn_id='aws_prod_probe_scraper'
    aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

    gke_location="us-central1-a"
    gke_cluster_name="bq-load-gke-1"

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

    probe_scraper = GKEPodOperator(
        task_id="probe_scraper",
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        name='probe-scraper',
        namespace='default',
        # This python job requires 13 GB of memory
        node_selectors={"nodepool" : "highmem"},
        # Due to the nature of the container run, we set get_logs to False,
        # To avoid urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes read)' errors
        # Where the pod continues to run, but airflow loses its connection and sets the status to Failed
        get_logs=False,
        image=probe_scraper_image,
        is_delete_operator_pod=True,
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
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        name='schema-generator-1',
        namespace='default',
        image='mozilla/mozilla-schema-generator:latest',
        is_delete_operator_pod=True,
        image_pull_policy='Always',
        env_vars={
            "MPS_SSH_KEY_BASE64": "{{ var.value.mozilla_pipeline_schemas_secret_git_sshkey_b64 }}",
            "MPS_REPO_URL": "git@github.com:mozilla-services/mozilla-pipeline-schemas.git",
            "MPS_BRANCH_SOURCE": "master",
            "MPS_BRANCH_PUBLISH": "generated-schemas",
        },
        dag=dag)

    schema_generator.set_upstream(probe_scraper)
