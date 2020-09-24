"""Airflow configuration for coordinating the Prio processing pipeline.

The prio-processor docs details the internals of the containerized application.
It contains storage and processing scripts that are coordinated by Airflow.

The graph has three phases: staging, publishing, and processing. Staging reads
batched prio pings and range-partitions across date, batch-id, and share-id. The
partitioned data sets are published to all subscribed servers. Each of the
servers runs a Kubernetes cluster running from the
`mozilla/prio-processor:latest` image. A container runs the processing pipeline,
starting with input at `{private-bucket}/raw/` and resulting in output at
`{private-bucket}/processed/`.

There are two sets of servers that run in Google Kubernetes Engine (GKE) under
Data Operations. The servers are assigned service accounts with access to a
private storage bucket associated with its project, and a pair of mutually
shared buckets. An administrative account oversees interop with ingestion,
including a process and transfer job from Firefox Telemetry into a receiving
bucket located in each server's project.

The SubDag operator simplifies the definition of each phase. The clusters are
ephemeral -- there are situations where clusters can become orphaned such as an
unclean shutdown of Airflow. These are usually resolved by rerunning the
individual SubDag, which should clean-up properly.

The following Airflow variables should be set:

    prio_private_key_hex_internal
    prio_public_key_hex_internal
    prio_private_key_hex_external
    prio_public_key_hex_external
    prio_shared_secret

These variables are encrypted and passed into the prio-processor container via
the environment.
"""
from datetime import datetime, timedelta
from os import environ
from functools import partial

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.operators import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from prio import dataproc, kubernetes

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

PRIO_ADMIN_CONN = "google_cloud_prio_admin"
PRIO_A_CONN = "google_cloud_prio_a"
PRIO_B_CONN = "google_cloud_prio_b"

PROJECT_ADMIN = GoogleCloudStorageHook(PRIO_ADMIN_CONN).project_id
PROJECT_A = GoogleCloudStorageHook(PRIO_A_CONN).project_id
PROJECT_B = GoogleCloudStorageHook(PRIO_B_CONN).project_id

SERVICE_ACCOUNT_ADMIN = f"prio-admin-runner@{PROJECT_ADMIN}.iam.gserviceaccount.com"
SERVICE_ACCOUNT_A = f"prio-runner-{ENVIRONMENT}-a@{PROJECT_A}.iam.gserviceaccount.com"
SERVICE_ACCOUNT_B = f"prio-runner-{ENVIRONMENT}-b@{PROJECT_B}.iam.gserviceaccount.com"

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

# Copy the dependencies necessary for running the `prio-processor staging` job
# into the relevant GCS locations. This includes the runner and the egg
# containing the bundled dependencies.

prio_staging_bootstrap = SubDagOperator(
    subdag=kubernetes.container_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="bootstrap",
        default_args=DEFAULT_ARGS,
        server_id="admin",
        gcp_conn_id=PRIO_ADMIN_CONN,
        service_account=SERVICE_ACCOUNT_ADMIN,
        arguments=[
            "bash",
            "-xc",
            f"source bin/dataproc; bootstrap gs://{BUCKET_BOOTSTRAP_ADMIN}",
        ],
        env_var=dict(SUBMODULE="origin"),
    ),
    task_id="bootstrap",
    dag=dag,
)

# Run the PySpark job for range-partioning Prio pings.
prio_staging = SubDagOperator(
    subdag=dataproc.spark_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="staging",
        default_args=DEFAULT_ARGS,
        gcp_conn_id=PRIO_ADMIN_CONN,
        service_account=SERVICE_ACCOUNT_ADMIN,
        main=f"gs://{BUCKET_BOOTSTRAP_ADMIN}/processor-origin.py",
        pyfiles=[f"gs://{BUCKET_BOOTSTRAP_ADMIN}/prio_processor.egg"],
        arguments=[
            "staging",
            "--date",
            "{{ ds }}",
            "--source",
            "bigquery",
            "--input",
            "moz-fx-data-shared-prod.payload_bytes_decoded.telemetry_telemetry__prio_v4",
            "--output",
            f"gs://{BUCKET_DATA_ADMIN}/staging/",
        ],
        bootstrap_bucket=f"gs://{BUCKET_BOOTSTRAP_ADMIN}",
        num_preemptible_workers=2,
    ),
    task_id="staging",
    default_args=DEFAULT_ARGS,
    dag=dag,
)


def transfer_data_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    source_bucket,
    destination_bucket,
    destination_bucket_prefix,
    app_name,
    submission_date,
    server_id,
    public_key_hex_external,
    google_cloud_storage_conn_id,
):
    """Copy the partitioned data from the staging bucket into the corresponding
    receiving buckets in each processor. The job then submits a `_SUCCESS` file
    which indicates the data is ready for processing.

    See the tests for the staging job for preprocessing convention:
    https://github.com/mozilla/prio-processor/blob/3cdc368707f8dc0f917d7b3d537c31645f4260f7/processor/tests/test_staging.py#L190-L205

    See the `bin/generate` script for an example around conventions around processing:
    https://github.com/mozilla/prio-processor/blob/1a4a58a738c3d39bfb04bbaa33a323412f1398ec/bin/generate#L53-L67
    """
    with DAG(f"{parent_dag_name}.{child_dag_name}", default_args=default_args) as dag:
        prefix = "/".join(
            [
                destination_bucket_prefix,
                public_key_hex_external,
                app_name,
                submission_date,
                "raw/shares",
            ]
        )
        transfer_dataset = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id="transfer_dataset",
            source_bucket=source_bucket,
            source_object=f"staging/submission_date={submission_date}/server_id={server_id}/*",
            destination_bucket=destination_bucket,
            destination_object=f"{prefix}/",
            google_cloud_storage_conn_id=google_cloud_storage_conn_id,
            dag=dag,
        )
        mark_dataset_success = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id="mark_dataset_success",
            source_bucket=source_bucket,
            source_object="staging/_SUCCESS",
            destination_bucket=destination_bucket,
            destination_object=f"{prefix}/_SUCCESS",
            google_cloud_storage_conn_id=google_cloud_storage_conn_id,
            dag=dag,
        )
        transfer_dataset >> mark_dataset_success
        return dag


transfer_data_subdag_partial = partial(
    transfer_data_subdag,
    parent_dag_name=dag.dag_id,
    default_args=DEFAULT_ARGS,
    source_bucket=BUCKET_DATA_ADMIN,
    destination_bucket_prefix=BUCKET_PREFIX,
    app_name=APP_NAME,
    submission_date="{{ ds }}",
    google_cloud_storage_conn_id=PRIO_ADMIN_CONN,
)

transfer_a = SubDagOperator(
    subdag=transfer_data_subdag_partial(
        child_dag_name="transfer_a",
        destination_bucket=BUCKET_PRIVATE_A,
        server_id="a",
        public_key_hex_external="{{ var.value.prio_public_key_hex_external }}",
    ),
    task_id="transfer_a",
    default_args=DEFAULT_ARGS,
    dag=dag,
)

transfer_b = SubDagOperator(
    subdag=transfer_data_subdag_partial(
        child_dag_name="transfer_b",
        destination_bucket=BUCKET_PRIVATE_B,
        server_id="b",
        public_key_hex_external="{{ var.value.prio_public_key_hex_internal }}",
    ),
    task_id="transfer_b",
    default_args=DEFAULT_ARGS,
    dag=dag,
)

# Initiate the processors by spinning up GKE clusters and running the main
# processing loop. These SubDags are coordinated by a higher level graph, but
# they can be decoupled and instead run on the same schedule. This structure is
# done for convenience, since both processors are running on the same
# infrastructure (but partitioned along project boundaries).

processor_a = SubDagOperator(
    subdag=kubernetes.container_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="processor_a",
        default_args=DEFAULT_ARGS,
        server_id="a",
        gcp_conn_id=PRIO_A_CONN,
        service_account=SERVICE_ACCOUNT_A,
        arguments=["bin/process"],
        env_vars={
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
    ),
    task_id="processor_a",
    dag=dag,
)

processor_b = SubDagOperator(
    subdag=kubernetes.container_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="processor_b",
        default_args=DEFAULT_ARGS,
        server_id="b",
        gcp_conn_id=PRIO_B_CONN,
        service_account=SERVICE_ACCOUNT_B,
        arguments=["bin/process"],
        env_vars={
            "APP_NAME": APP_NAME,
            "SUBMISSION_DATE": "{{ ds }}",
            "DATA_CONFIG": "/app/config/content.json",
            "SERVER_ID": "B",
            "SHARED_SECRET": "{{ var.value.prio_shared_secret }}",
            "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_external }}",
            "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_external }}",
            "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
            "BUCKET_INTERNAL_PRIVATE": "gs://" + BUCKET_PRIVATE_B,
            "BUCKET_INTERNAL_SHARED": "gs://" + BUCKET_SHARED_B,
            "BUCKET_EXTERNAL_SHARED": "gs://" + BUCKET_SHARED_A,
            "BUCKET_PREFIX": BUCKET_PREFIX,
            # 15 minutes of time-out
            "RETRY_LIMIT": "90",
            "RETRY_DELAY": "10",
            "RETRY_BACKOFF_EXPONENT": "1",
        },
    ),
    task_id="processor_b",
    dag=dag,
)

# Take the resulting aggregates and insert them into a BigQuery table. This
# table is effectively append-only, so rerunning the dag will cause duplicate
# results. In practice, rerunning the DAG is problematic when operation is
# distributed.
insert_into_bigquery = SubDagOperator(
    subdag=kubernetes.container_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="insert_into_bigquery",
        default_args=DEFAULT_ARGS,
        server_id="admin",
        gcp_conn_id=PRIO_ADMIN_CONN,
        service_account=SERVICE_ACCOUNT_ADMIN,
        arguments=["bash", "-c", "bin/insert"],
        env_vars={
            "DATA_CONFIG": "/app/config/content.json",
            "ORIGIN_CONFIG": "/app/config/telemetry_origin_data_inc.json",
            "BUCKET_INTERNAL_PRIVATE": "gs://" + BUCKET_PRIVATE_A,
            "DATASET": "telemetry",
            "TABLE": "origin_content_blocking",
            "BQ_REPLACE": "false",
            "GOOGLE_APPLICATION_CREDENTIALS": "",
        },
    ),
    task_id="insert_into_bigquery",
    dag=dag,
)


# Stitch the operators together into a directed acyclic graph.
prio_staging_bootstrap >> prio_staging
prio_staging >> transfer_a >> processor_a
prio_staging >> transfer_b >> processor_b

# Wait for both parents to finish (though only a dependency on processor a is
# technically necessary)
processor_a >> insert_into_bigquery
processor_b >> insert_into_bigquery
