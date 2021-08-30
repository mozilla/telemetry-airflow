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

Note that the container subdag may require running multiple tasks in parallel.
This can be achieved by getting the default executor as per:
https://stackoverflow.com/a/56069569

The following Airflow variables should be set:

    prio_private_key_hex_internal
    prio_public_key_hex_internal
    prio_private_key_hex_external
    prio_public_key_hex_external
    prio_shared_secret

These variables are encrypted and passed into the prio-processor container via
the environment.
"""
from functools import partial

from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.operators import DummyOperator, PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from prio import dataproc, kubernetes


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


def ingestion_subdag(
    dag,
    default_args,
    gcp_conn_id,
    service_account,
    bucket_bootstrap_admin,
    bucket_data_admin,
    bucket_prefix,
    app_name,
    bucket_private_internal,
    bucket_private_external,
    public_key_hex_internal,
    public_key_hex_external,
    # the environment variables are primarily used by a container that is using
    # mc to copy data between buckets
    env_vars={},
    # is the external party running minio (i.e. should we transfer data using mc)
    is_transfer_external_minio=False,
):
    # Copy the dependencies necessary for running the `prio-processor staging` job
    # into the relevant GCS locations. This includes the runner and the egg
    # containing the bundled dependencies.

    prio_staging_bootstrap = SubDagOperator(
        subdag=kubernetes.container_subdag(
            parent_dag_name=dag.dag_id,
            child_dag_name="bootstrap",
            default_args=default_args,
            server_id="admin",
            gcp_conn_id=gcp_conn_id,
            service_account=service_account,
            arguments=[
                "bash",
                "-xc",
                f"source bin/dataproc; bootstrap gs://{bucket_bootstrap_admin}",
            ],
            env_var=dict(SUBMODULE="origin"),
            # prevent minio from spawnning by changing the job kind from prio-processor
            job_kind="bootstrap",
        ),
        task_id="bootstrap",
        dag=dag,
    )

    # Run the PySpark job for range-partioning Prio pings.
    prio_staging = SubDagOperator(
        subdag=dataproc.spark_subdag(
            parent_dag_name=dag.dag_id,
            child_dag_name="staging",
            default_args=default_args,
            gcp_conn_id=gcp_conn_id,
            service_account=service_account,
            main=f"gs://{bucket_bootstrap_admin}/processor-origin.py",
            pyfiles=[f"gs://{bucket_bootstrap_admin}/prio_processor.egg"],
            arguments=[
                "staging",
                "--date",
                "{{ ds }}",
                "--source",
                "bigquery",
                "--input",
                "moz-fx-data-shared-prod.payload_bytes_decoded.telemetry_telemetry__prio_v4",
                "--output",
                f"gs://{bucket_data_admin}/staging/",
            ],
            bootstrap_bucket=f"gs://{bucket_bootstrap_admin}",
            # TODO: reconfigure the number of workers
            num_preemptible_workers=2,
        ),
        task_id="staging",
        default_args=default_args,
        dag=dag,
    )

    transfer_data_subdag_partial = partial(
        transfer_data_subdag,
        parent_dag_name=dag.dag_id,
        default_args=default_args,
        source_bucket=bucket_data_admin,
        destination_bucket_prefix=bucket_prefix,
        app_name=app_name,
        submission_date="{{ ds }}",
        google_cloud_storage_conn_id=gcp_conn_id,
    )

    transfer_a = SubDagOperator(
        subdag=transfer_data_subdag_partial(
            child_dag_name="transfer_a",
            destination_bucket=bucket_private_internal,
            server_id="a",
            public_key_hex_external=public_key_hex_external,
        ),
        task_id="transfer_a",
        default_args=default_args,
        dag=dag,
    )

    # The ingestion service needs to send data to the external party. See the
    # generate script for some usage of mc.
    # https://github.com/mozilla/prio-processor/blob/v4.1.2/bin/generate
    if is_transfer_external_minio:
        src_path = f"internal/{bucket_data_admin}/staging/submission_date={'{{ ds }}'}/server_id=b"
        dst_path = f"external/{bucket_private_external}/{bucket_prefix}/{public_key_hex_internal}/{app_name}/{'{{ ds }}'}/raw/shares"
        transfer_b = SubDagOperator(
            subdag=kubernetes.container_subdag(
                parent_dag_name=dag.dag_id,
                child_dag_name="transfer_b",
                default_args=default_args,
                server_id="admin",
                gcp_conn_id=gcp_conn_id,
                service_account=service_account,
                arguments=[
                    "bash",
                    "-c",
                    # wait for the minio container to come online, 10 seconds is
                    # sometimes not enough
                    "sleep 30; "
                    # mitigate the -e setting inside of configure-mc, so we will
                    # always run the last statement in this job. In general,
                    # handling error codes will be messy with the way that the
                    # sidecar containers are implemented, so manually killing
                    # the pod may be required if there is an intermediate
                    # failure.
                    "source bin/configure-mc || exec -a minio-done sleep 10; set +e; "
                    f"mc mirror --remove --overwrite {src_path} {dst_path}; "
                    f"touch _SUCCESS; mc cp _SUCCESS {dst_path}; "
                    "exec -a minio-done bash -c 'sleep 10 && exit 0'",
                ],
                env_vars=env_vars,
            ),
            task_id="transfer_b",
            dag=dag,
        )
    else:
        # using the transfer subdag is much easier to debug since we don't need
        # to spin up an entirely new kubernetes cluster
        transfer_b = SubDagOperator(
            subdag=transfer_data_subdag_partial(
                child_dag_name="transfer_b",
                destination_bucket=bucket_private_external,
                server_id="b",
                public_key_hex_external=public_key_hex_internal,
            ),
            task_id="transfer_b",
            default_args=default_args,
            dag=dag,
        )

    transfer_complete = DummyOperator(task_id="transfer_complete", dag=dag)

    prio_staging_bootstrap >> prio_staging
    prio_staging >> transfer_a
    prio_staging >> transfer_b
    transfer_a >> transfer_complete
    transfer_b >> transfer_complete
    return transfer_complete


def prio_processor_subdag(
    dag, default_args, gcp_conn_id, service_account, server_id, env_vars
):
    return SubDagOperator(
        subdag=kubernetes.container_subdag(
            parent_dag_name=dag.dag_id,
            child_dag_name=f"processor_{server_id}",
            default_args=default_args,
            gcp_conn_id=gcp_conn_id,
            service_account=service_account,
            server_id=server_id,
            arguments=["bash", "-c", "bin/process; exec -a minio-done sleep 10"],
            env_vars=env_vars,
        ),
        task_id=f"processor_{server_id}",
        dag=dag,
    )


def load_bigquery_subdag(dag, default_args, gcp_conn_id, service_account, env_vars):
    # Take the resulting aggregates and insert them into a BigQuery table. This
    # table is effectively append-only, so rerunning the dag will cause duplicate
    # results. In practice, rerunning the DAG is problematic when operation is
    # distributed.
    return SubDagOperator(
        subdag=kubernetes.container_subdag(
            parent_dag_name=dag.dag_id,
            child_dag_name="insert_into_bigquery",
            default_args=default_args,
            server_id="admin",
            gcp_conn_id=gcp_conn_id,
            service_account=service_account,
            arguments=["bash", "-c", "bin/insert"],
            env_vars=env_vars,
        ),
        task_id="insert_into_bigquery",
        dag=dag,
    )
