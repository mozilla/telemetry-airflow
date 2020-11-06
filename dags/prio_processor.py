from datetime import datetime, timedelta
from os import environ

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from prio.processor import ingestion_subdag, load_bigquery_subdag, prio_processor_subdag

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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "dagrun_timeout": timedelta(hours=4),
}

# use a less than desirable method of generating the service account name
IS_DEV = environ.get("DEPLOY_ENVIRONMENT") != "prod"
ENVIRONMENT = "dev" if IS_DEV else "prod"

PRIO_ADMIN_CONN = "google_cloud_prio_admin"
PRIO_A_CONN = "google_cloud_prio_a"

PROJECT_ADMIN = GoogleCloudStorageHook(PRIO_ADMIN_CONN).project_id
PROJECT_A = GoogleCloudStorageHook(PRIO_A_CONN).project_id

SERVICE_ACCOUNT_ADMIN = f"prio-admin-runner@{PROJECT_ADMIN}.iam.gserviceaccount.com"
SERVICE_ACCOUNT_A = f"prio-runner-{ENVIRONMENT}-a@{PROJECT_A}.iam.gserviceaccount.com"

# Private bucket of server B is necessary for transfer
BUCKET_PRIVATE_A = f"moz-fx-prio-{ENVIRONMENT}-a-private"
BUCKET_PRIVATE_B = f"moz-fx-prio-{ENVIRONMENT}-b-private"
BUCKET_SHARED_A = f"moz-fx-prio-{ENVIRONMENT}-a-shared"
BUCKET_SHARED_B = f"moz-fx-prio-{ENVIRONMENT}-b-shared"
BUCKET_DATA_ADMIN = f"moz-fx-data-{ENVIRONMENT}-prio-data"
BUCKET_BOOTSTRAP_ADMIN = f"moz-fx-data-{ENVIRONMENT}-prio-bootstrap"

APP_NAME = "origin-telemetry"
BUCKET_PREFIX = "data/v1"

# https://airflow.apache.org/faq.html#how-can-my-airflow-dag-run-faster
# max_active_runs controls the number of DagRuns at a given time.
dag = DAG(
    dag_id="prio_processor",
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
)

ingest = ingestion_subdag(
    dag,
    DEFAULT_ARGS,
    PRIO_ADMIN_CONN,
    SERVICE_ACCOUNT_ADMIN,
    BUCKET_BOOTSTRAP_ADMIN,
    BUCKET_DATA_ADMIN,
    BUCKET_PREFIX,
    APP_NAME,
    BUCKET_PRIVATE_A,
    BUCKET_PRIVATE_B,
    "{{ var.value.prio_public_key_hex_internal }}",
    "{{ var.value.prio_public_key_hex_external }}",
)

processor_a = prio_processor_subdag(
    dag,
    DEFAULT_ARGS,
    PRIO_A_CONN,
    SERVICE_ACCOUNT_A,
    "a",
    {
        "APP_NAME": APP_NAME,
        "SUBMISSION_DATE": "{{ ds }}",
        "DATA_CONFIG": "/app/config/content.json",
        "SERVER_ID": "A",
        "SHARED_SECRET": "{{ var.value.prio_shared_secret }}",
        "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_internal }}",
        "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
        "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_external }}",
        "BUCKET_INTERNAL_PRIVATE": "gs://" + BUCKET_PRIVATE_A,
        "BUCKET_INTERNAL_SHARED": "gs://" + BUCKET_SHARED_A,
        "BUCKET_EXTERNAL_SHARED": "gs://" + BUCKET_SHARED_B,
        "BUCKET_PREFIX": BUCKET_PREFIX,
        # 15 minutes of timeout
        "RETRY_LIMIT": "90",
        "RETRY_DELAY": "10",
        "RETRY_BACKOFF_EXPONENT": "1",
    },
)

load_bigquery = load_bigquery_subdag(
    dag,
    DEFAULT_ARGS,
    PRIO_ADMIN_CONN,
    SERVICE_ACCOUNT_ADMIN,
    env_vars={
        "APP_NAME": APP_NAME,
        "SUBMISSION_DATE": "{{ ds }}",
        "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_external }}",
        "DATA_CONFIG": "/app/config/content.json",
        "ORIGIN_CONFIG": "/app/config/telemetry_origin_data_inc.json",
        "BUCKET_INTERNAL_PRIVATE": "gs://" + BUCKET_PRIVATE_A,
        "DATASET": "telemetry",
        "TABLE": "origin_content_blocking",
        "BQ_REPLACE": "false",
        "GOOGLE_APPLICATION_CREDENTIALS": "",
    },
)

ingest >> processor_a
processor_a >> load_bigquery
