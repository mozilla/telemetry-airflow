import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from operators.gcp_container_operator import GKEPodOperator

DOCS = """\
# Probe Scraper

## Debugging failures

Probe Scraper application logs aren't available via the Airflow web console. In
order to access them, do the following:

1. Navigate to [this page](https://workflow.telemetry.mozilla.org/tree?dag_id=probe_scraper)
2. Click the `probe_scraper` DAG that failed, followed by `View Log`
3. Search for logs like `Event: probe-scraper-[HEX-STRING] had an event of type Pending` and note the container id
4. Navigate to the [Google Cloud Logging console](https://console.cloud.google.com/logs/query?project=moz-fx-data-derived-datasets)
If you can't access these logs but think you should be able to, [contact Data SRE](https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=DOPS&title=Contacting+Data+SRE).
5. Search for the following, replacing `POD_NAME_FROM_AIRFLOW_LOGS` with the string from (3):

```
resource.type="k8s_container"
resource.labels.project_id="moz-fx-data-derived-datasets"
resource.labels.location="us-central1-a"
resource.labels.cluster_name="bq-load-gke-1"
resource.labels.namespace_name="default"
resource.labels.pod_name="POD_NAME_FROM_AIRFLOW_LOGS" severity>=DEFAULT
```

Adjust the time window as needed and you should be able to see logs associated with the failure.
"""

default_args = {
    'owner': 'ascholtz@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG('probe_scraper',
         doc_md=DOCS,
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
                 'limit_memory':'20480Mi', 'limit_cpu': None, 'limit_gpu': None}

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
        email=['telemetry-client-dev@mozilla.com', 'aplacitelli@mozilla.com', 'hwoo@mozilla.com'],
        env_vars={
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key
        },
        dag=dag)

    schema_generator = GKEPodOperator(
        email=['amiyaguchi@mozilla.com', 'dataops+alerts@mozilla.com'],
        task_id='mozilla_schema_generator',
        name='schema-generator-1',
        image='mozilla/mozilla-schema-generator:latest',
        env_vars={
            "MPS_SSH_KEY_BASE64": "{{ var.value.mozilla_pipeline_schemas_secret_git_sshkey_b64 }}",
            "MPS_REPO_URL": "git@github.com:mozilla-services/mozilla-pipeline-schemas.git",
            "MPS_BRANCH_SOURCE": "main",
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

    delay_python_task = PythonOperator(
        task_id="wait_for_30_minutes",
        dag=dag,
        python_callable=lambda: time.sleep(60 * 30))
    
    probe_scraper >> delay_python_task

    gcp_gke_conn_id = "google_cloud_airflow_gke"
    lookml_generator_prod = GKEPodOperator(
        email=["frank@mozilla.com", "dataops+alerts@mozilla.com"],
        task_id="lookml_generator",
        name="lookml-generator-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/lookml-generator:" + Variable.get("lookml_generator_release_str"),
        startup_timeout_seconds=500,
        gcp_conn_id=gcp_gke_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_gke_conn_id).project_id,
        cluster_name="workloads-prod-v1",
        location="us-west1",
        dag=dag,
        env_vars={
            "GIT_SSH_KEY_BASE64": Variable.get("looker_repos_secret_git_ssh_key_b64"),
            "HUB_REPO_URL": "git@github.com:mozilla/looker-hub.git",
            "HUB_BRANCH_SOURCE": "base",
            "HUB_BRANCH_PUBLISH": "main",
            "SPOKE_REPO_URL": "git@github.com:mozilla/looker-spoke-default.git",
            "SPOKE_BRANCH_PUBLISH": "main",
            "LOOKER_INSTANCE_URI": "https://mozilla.cloud.looker.com",
            "LOOKER_API_CLIENT_ID": Variable.get("looker_api_client_id_prod"),
            "LOOKER_API_CLIENT_SECRET": Variable.get("looker_api_client_secret_prod"),
            "GITHUB_ACCESS_TOKEN": Variable.get("dataops_looker_github_secret_access_token"),
            "UPDATE_SPOKE_BRANCHES": "true",
        }
    )

    delay_python_task >> lookml_generator_prod

    lookml_generator_staging = GKEPodOperator(
        email=["ascholtz@mozilla.com", "dataops+alerts@mozilla.com"],
        task_id="lookml_generator_staging",
        name="lookml-generator-staging-1",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/lookml-generator:latest",
        gcp_conn_id=gcp_gke_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_gke_conn_id).project_id,
        cluster_name="workloads-prod-v1",
        location="us-west1",
        dag=dag,
        env_vars={
            "GIT_SSH_KEY_BASE64": Variable.get("looker_repos_secret_git_ssh_key_b64"),
            "HUB_REPO_URL": "git@github.com:mozilla/looker-hub.git",
            "HUB_BRANCH_SOURCE": "base",
            "HUB_BRANCH_PUBLISH": "main-stage",
            "SPOKE_REPO_URL": "git@github.com:mozilla/looker-spoke-default.git",
            "SPOKE_BRANCH_PUBLISH": "main-stage",
            "LOOKER_INSTANCE_URI": "https://mozillastaging.cloud.looker.com",
            "LOOKER_API_CLIENT_ID": Variable.get("looker_api_client_id_staging"),
            "LOOKER_API_CLIENT_SECRET": Variable.get("looker_api_client_secret_staging"),
            "GITHUB_ACCESS_TOKEN": Variable.get("dataops_looker_github_secret_access_token"),
            "UPDATE_SPOKE_BRANCHES": "true",
        }
    )

    delay_python_task >> lookml_generator_staging

    # This emits a POST request to a netlify webhook URL that triggers a new
    # build of the glean dictionary. We do this after the schema generator has
    # finished running as the dictionary uses the new schema files as part of
    # said build.
    glean_dictionary_netlify_build = SimpleHttpOperator(
        http_conn_id="http_netlify_build_webhook",
        endpoint=Variable.get("glean_dictionary_netlify_build_webhook_id"),
        method="POST",
        data={},
        email=["wlach@mozilla.com", "dataops+alerts@mozilla.com"],
        task_id="glean_dictionary_build",
        dag=dag,
    )

    probe_scraper >> glean_dictionary_netlify_build

