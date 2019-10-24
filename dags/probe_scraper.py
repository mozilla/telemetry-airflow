from airflow import DAG
from datetime import timedelta, datetime
from operators.emr_spark_operator import EMRSparkOperator
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG('probe_scraper',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    probe_scraper = EMRSparkOperator(
        task_id="probe_scraper",
        job_name="Probe Scraper",
        execution_timeout=timedelta(hours=4),
        instance_count=1,
        email=['telemetry-client-dev@mozilla.com', 'aplacitelli@mozilla.com', 'frank@mozilla.com'],
        env={},
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/probe_scraper.sh",
        output_visibility="public",
        dag=dag)

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    schema_generator = GKEPodOperator(
        email=['frank@mozilla.com'],
        task_id='mozilla_schema_generator',
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location='us-central1-a',
        cluster_name='bq-load-gke-1',
        name='schema-generator-1',
        namespace='default',
        image='mozilla/mozilla-schema-generator:latest',
        image_pull_policy='Always',
        is_delete_operator_pod=True,
        env_vars={
            "MPS_SSH_KEY_BASE64": "{{ var.value.mozilla_pipeline_schemas_secret_git_sshkey_b64 }}",
            "MPS_REPO_URL": "git@github.com:mozilla-services/mozilla-pipeline-schemas.git",
            "MPS_BRANCH_SOURCE": "master",
            "MPS_BRANCH_PUBLISH": "generated-schemas",
        },
        dag=dag)

    schema_generator.set_upstream(probe_scraper)
