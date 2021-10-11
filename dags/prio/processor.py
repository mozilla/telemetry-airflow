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
from functools import partial

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
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
    gcp_conn_id,
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
        transfer_dataset = GCSToGCSOperator(
            task_id="transfer_dataset",
            source_bucket=source_bucket,
            source_object=f"staging/submission_date={submission_date}/server_id={server_id}/*",
            destination_bucket=destination_bucket,
            destination_object=f"{prefix}/",
            gcp_conn_id=gcp_conn_id,
            dag=dag,
        )
        mark_dataset_success = GCSToGCSOperator(
            task_id="mark_dataset_success",
            source_bucket=source_bucket,
            source_object="staging/_SUCCESS",
            destination_bucket=destination_bucket,
            destination_object=f"{prefix}/_SUCCESS",
            gcp_conn_id=gcp_conn_id,
            dag=dag,
        )
        transfer_dataset >> mark_dataset_success
        return dag


def ingestion_subdag(
    dag,
    default_args,
    gcp_conn_id,
    project_id,
    service_account,
    bucket_bootstrap_admin,
    bucket_data_admin,
    bucket_prefix,
    app_name,
    bucket_private_internal,
    bucket_private_external,
    public_key_hex_internal,
    public_key_hex_external,
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
            project_id=project_id,
            service_account=service_account,
            arguments=[
                "bash",
                "-xc",
                f"source bin/dataproc; bootstrap gs://{bucket_bootstrap_admin}",
            ],
            env_vars=dict(SUBMODULE="origin"),
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
            project_id=project_id,
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
        gcp_conn_id=gcp_conn_id,
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
    dag, default_args, gcp_conn_id, project_id, service_account, server_id, env_vars
):
    return SubDagOperator(
        subdag=kubernetes.container_subdag(
            parent_dag_name=dag.dag_id,
            child_dag_name=f"processor_{server_id}",
            default_args=default_args,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            service_account=service_account,
            server_id=server_id,
            arguments=["bin/process"],
            env_vars=env_vars,
        ),
        task_id=f"processor_{server_id}",
        dag=dag,
    )


def load_bigquery_subdag(dag, default_args, gcp_conn_id, project_id, service_account, env_vars):
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
            project_id=project_id,
            service_account=service_account,
            arguments=["bash", "-c", "bin/insert"],
            env_vars=env_vars,
        ),
        task_id="insert_into_bigquery",
        dag=dag,
    )
