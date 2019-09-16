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
    "depends_on_past": True,
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
    "schedule_interval": "@daily",
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

SERVICE_ACCOUNT_ADMIN = "prio-admin-runner@{}.iam.gserviceaccount.com".format(
    PROJECT_ADMIN
)
SERVICE_ACCOUNT_A = "prio-runner-{}-a@{}.iam.gserviceaccount.com".format(
    ENVIRONMENT, PROJECT_A
)
SERVICE_ACCOUNT_B = "prio-runner-{}-b@{}.iam.gserviceaccount.com".format(
    ENVIRONMENT, PROJECT_B
)

BUCKET_PRIVATE_A = "moz-fx-prio-{}-a-private".format(ENVIRONMENT)
BUCKET_PRIVATE_B = "moz-fx-prio-{}-b-private".format(ENVIRONMENT)
BUCKET_SHARED_A = "moz-fx-prio-{}-a-shared".format(ENVIRONMENT)
BUCKET_SHARED_B = "moz-fx-prio-{}-b-shared".format(ENVIRONMENT)
BUCKET_DATA_ADMIN = "moz-fx-data-{}-prio-data".format(ENVIRONMENT)
BUCKET_BOOTSTRAP_ADMIN = "moz-fx-data-{}-prio-bootstrap".format(ENVIRONMENT)

dag = DAG(dag_id="prio_processor", max_active_runs=1, default_args=DEFAULT_ARGS)


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
            "-c",
            "cd processor; prio-processor bootstrap --output gs://{}".format(
                BUCKET_BOOTSTRAP_ADMIN
            ),
        ],
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
        main="gs://{}/runner.py".format(BUCKET_BOOTSTRAP_ADMIN),
        pyfiles=["gs://{}/prio_processor.egg".format(BUCKET_BOOTSTRAP_ADMIN)],
        arguments=[
            "staging",
            "--date",
            "{{ ds }}",
            "--source",
            "bigquery",
            "--input",
            "moz-fx-data-shared-prod.payload_bytes_decoded.telemetry_telemetry__prio_v4",
            "--output",
            "gs://{}/staging/".format(BUCKET_DATA_ADMIN),
        ],
        num_preemptible_workers=2,
    ),
    task_id="staging",
    default_args=DEFAULT_ARGS,
    dag=dag,
)

# Copy the partitioned data from the staging bucket into the corresponding
# receiving buckets in each processor. The job then submits a `_SUCCESS` file
# which indicates the data is ready for processing.
#
# See: https://github.com/mozilla/prio-processor/blob/3cdc368707f8dc0f917d7b3d537c31645f4260f7/processor/tests/test_staging.py#L190-L205

# NOTE: GoogleCloudStorageDeleteOperator does not exist in v1.10.2
def clean_buckets(google_cloud_storage_conn_id, private_bucket, shared_bucket):
    import logging

    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id)
    total = 0

    # clean the entire bucket
    private = [(private_bucket, name) for name in hook.list(private_bucket)]
    shared = [(shared_bucket, name) for name in hook.list(shared_bucket)]

    for bucket_name, object_name in private + shared:
        logging.info("Deleting gs://{}/{}".format(bucket_name, object_name))
        hook.delete(bucket_name, object_name)
        total += 1
    logging.info("Deleted {} objects".format(total))


clean_processor_a = PythonOperator(
    task_id="clean_processor_a",
    python_callable=clean_buckets,
    op_kwargs={
        "private_bucket": BUCKET_PRIVATE_A,
        "shared_bucket": BUCKET_SHARED_A,
        "google_cloud_storage_conn_id": PRIO_A_CONN,
    },
    dag=dag,
)

clean_processor_b = PythonOperator(
    task_id="clean_processor_b",
    python_callable=clean_buckets,
    op_kwargs={
        "private_bucket": BUCKET_PRIVATE_B,
        "shared_bucket": BUCKET_SHARED_B,
        "google_cloud_storage_conn_id": PRIO_B_CONN,
    },
    dag=dag,
)

load_processor_a = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="load_processor_a",
    source_bucket=BUCKET_DATA_ADMIN,
    source_object="staging/submission_date={{ ds }}/server_id=a/*",
    destination_bucket=BUCKET_PRIVATE_A,
    destination_object="raw/submission_date={{ ds }}/",
    google_cloud_storage_conn_id=PRIO_ADMIN_CONN,
    dag=dag,
)

load_processor_b = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="load_processor_b",
    source_bucket=BUCKET_DATA_ADMIN,
    source_object="staging/submission_date={{ ds }}/server_id=b/*",
    destination_bucket=BUCKET_PRIVATE_B,
    destination_object="raw/submission_date={{ ds }}/",
    google_cloud_storage_conn_id=PRIO_ADMIN_CONN,
    dag=dag,
)

trigger_processor_a = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="trigger_processor_a",
    source_bucket=BUCKET_DATA_ADMIN,
    source_object="staging/_SUCCESS",
    destination_bucket=BUCKET_PRIVATE_A,
    destination_object="raw/_SUCCESS",
    google_cloud_storage_conn_id=PRIO_ADMIN_CONN,
    dag=dag,
)

trigger_processor_b = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="trigger_processor_b",
    source_bucket=BUCKET_DATA_ADMIN,
    source_object="staging/_SUCCESS",
    destination_bucket=BUCKET_PRIVATE_B,
    destination_object="raw/_SUCCESS",
    google_cloud_storage_conn_id=PRIO_ADMIN_CONN,
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
        arguments=["processor/bin/process"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config/content.json",
            "SERVER_ID": "A",
            "SHARED_SECRET": "{{ var.value.prio_shared_secret }}",
            "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_internal }}",
            "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
            "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_external }}",
            "BUCKET_INTERNAL_PRIVATE": BUCKET_PRIVATE_A,
            "BUCKET_INTERNAL_SHARED": BUCKET_SHARED_A,
            "BUCKET_EXTERNAL_SHARED": BUCKET_SHARED_B,
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
        arguments=["processor/bin/process"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config/content.json",
            "SERVER_ID": "B",
            "SHARED_SECRET": "{{ var.value.prio_shared_secret }}",
            "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_external }}",
            "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_external }}",
            "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
            "BUCKET_INTERNAL_PRIVATE": BUCKET_PRIVATE_B,
            "BUCKET_INTERNAL_SHARED": BUCKET_SHARED_B,
            "BUCKET_EXTERNAL_SHARED": BUCKET_SHARED_A,
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
        arguments=["bash", "-c", "cd processor; bin/insert"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config/content.json",
            "ORIGIN_CONFIG": "/app/processor/config/telemetry_origin_data_inc.json",
            "BUCKET_INTERNAL_PRIVATE": BUCKET_PRIVATE_A,
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
prio_staging >> clean_processor_a >> load_processor_a >> trigger_processor_a >> processor_a
prio_staging >> clean_processor_b >> load_processor_b >> trigger_processor_b >> processor_b

# Wait for both parents to finish (though only a dependency on processor a is
# technically necessary)
processor_a >> insert_into_bigquery
processor_b >> insert_into_bigquery
