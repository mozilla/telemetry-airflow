from datetime import datetime, timedelta
from os import environ

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from prio.processor import prio_processor_subdag

DEFAULT_ARGS = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 22),
    "email": [
        "amiyaguchi@mozilla.com",
        "hwoo@mozilla.com",
        "dataops+alerts@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "dagrun_timeout": timedelta(hours=4),
}

# use a less than desirable method of generating the service account name
IS_DEV = environ.get("DEPLOY_ENVIRONMENT") != "prod"
ENVIRONMENT = "dev" if IS_DEV else "prod"
PRIO_B_CONN = "google_cloud_prio_b"
PROJECT_B = GoogleCloudStorageHook(PRIO_B_CONN).project_id
SERVICE_ACCOUNT_B = f"prio-runner-{ENVIRONMENT}-b@{PROJECT_B}.iam.gserviceaccount.com"
BUCKET_PRIVATE_B = f"moz-fx-prio-{ENVIRONMENT}-b-private"
BUCKET_SHARED_A = f"moz-fx-prio-{ENVIRONMENT}-a-shared"
BUCKET_SHARED_B = f"moz-fx-prio-{ENVIRONMENT}-b-shared"

APP_NAME = "origin-telemetry"
BUCKET_PREFIX = "data/v1"

# https://airflow.apache.org/faq.html#how-can-my-airflow-dag-run-faster
# max_active_runs controls the number of DagRuns at a given time.
dag = DAG(
    dag_id="prio_processor_external",
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    # 30 minute delay to account for data being transferred in
    schedule_interval="30 0 * * *",
)

username = "testtest"
password = "testtesttest"

processor_b = prio_processor_subdag(
    dag,
    DEFAULT_ARGS,
    PRIO_B_CONN,
    SERVICE_ACCOUNT_B,
    "b",
    {
        # required for minio in the pod_mutation_hook
        "PROJECT_ID": PROJECT_B,
        # used for the minio instance
        "MINIO_ROOT_USER": username,
        "MINIO_ROOT_PASSWORD": password,
        # configuration for the server
        "APP_NAME": APP_NAME,
        "SUBMISSION_DATE": "{{ ds }}",
        "DATA_CONFIG": "/app/config/content.json",
        "SERVER_ID": "B",
        "SHARED_SECRET": "{{ var.value.prio_shared_secret }}",
        "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_external }}",
        "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_external }}",
        "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
        # configuration to minio gateway
        "BUCKET_INTERNAL_ACCESS_KEY": username,
        "BUCKET_INTERNAL_SECRET_KEY": password,
        "BUCKET_INTERNAL_ENDPOINT": "http://localhost:9000",
        "BUCKET_EXTERNAL_ACCESS_KEY": username,
        "BUCKET_EXTERNAL_SECRET_KEY": password,
        "BUCKET_EXTERNAL_ENDPOINT": "http://localhost:9000",
        # other bucket information
        "BUCKET_INTERNAL_INGEST": BUCKET_PRIVATE_B,
        "BUCKET_INTERNAL_PRIVATE": BUCKET_PRIVATE_B,
        "BUCKET_INTERNAL_SHARED": BUCKET_SHARED_B,
        "BUCKET_EXTERNAL_SHARED": BUCKET_SHARED_A,
        "BUCKET_PREFIX": BUCKET_PREFIX,
        # 15 minutes of time-out
        "RETRY_LIMIT": "180",
        "RETRY_DELAY": "10",
        "RETRY_BACKOFF_EXPONENT": "1",
    },
)
