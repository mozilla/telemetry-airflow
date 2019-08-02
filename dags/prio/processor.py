"""Airflow configuration for coordinating the Prio processing pipeline.

The prio-processor docs details the internals of the containerized application.
It contains storage and processing scripts that are coordinated by Airflow.

The graph has three phases: partitioning, publishing, and processing.
Partitioning reads batched prio pings and range-partitions across date,
batch-id, and share-id. The partitioned data sets are published to all
subscribed servers. Each of the servers runs a Kubernetes cluster running from
the `mozilla/prio-processor:latest` image. A container runs the processing
pipeline, starting with input at `{private-bucket}/raw/` and resulting in output
at `{private-bucket}/processed/`.

There are two sets of servers that run in Google Kubernetes Engine (GKE) under
Data Operations. The servers are assigned service accounts with access to a
private storage bucket associated with its project, and a pair of mutually
shared buckets. An administrative account oversees interop with ingestion,
including a process and transfer job from Firefox Telemetry into a receiving
bucket located in each server's project.

The SubDag operator simplifies the definition of each phase. The clusters are
ephemeral -- there are situations where clusters can become orphaned. These are
usually resolved by rerunning the individual SubDag, which should clean-up
properly.
"""
from datetime import datetime, timedelta
from os import environ

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from operators.gcp_container_operator import (
    GKEClusterCreateOperator,
    GKEClusterDeleteOperator,
    GKEPodOperator,
)
from utils.gke import create_gke_config

DEFAULT_ARGS = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 24),
    "email": [
        "amiyaguchi@mozilla.com",
        "hwoo@mozilla.com",
        "dataops+alerts@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


def prio_processor(
    server_id,
    cluster_name,
    gcp_conn_id,
    service_account,
    env_vars,
    arguments,
    *args,
    **kwargs
):
    assert server_id in ["a", "b", "admin"]

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    gke_dag = DAG(
        "prio.gke_prio_{}".format(server_id),
        # TODO: refactor to inherit from the main_dag
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=6),
        schedule_interval="@weekly",
    )

    shared_config = {
        "project_id": connection.project_id,
        "location": "us-west1-b",
        "gcp_conn_id": gcp_conn_id,
        "dag": gke_dag,
    }

    create_gke_cluster = GKEClusterCreateOperator(
        task_id="create_gke_cluster",
        body=create_gke_config(
            name=cluster_name,
            service_account=service_account,
            owner_label="hwoo",
            team_label="dataops",
            # DataProc clusters require VPC with auto-created subnets
            subnetwork="default" if server_id == "admin" else "gke-subnet",
            is_dev=environ.get("DEPLOY_ENVIRONMENT") == "dev",
        ),
        **shared_config
    )

    run_prio = GKEPodOperator(
        task_id="run_prio_{}".format(server_id),
        cluster_name=cluster_name,
        name="run-prio-project-{}".format(server_id),
        namespace="default",
        image="mozilla/prio-processor:latest",
        arguments=arguments,
        env_vars=env_vars,
        **shared_config
    )

    delete_gke_cluster = GKEClusterDeleteOperator(
        task_id="delete_gke_cluster",
        name=cluster_name,
        trigger_rule="all_done",
        **shared_config
    )

    create_gke_cluster >> run_prio >> delete_gke_cluster
    return gke_dag


def prio_staging(
    parent_dag_name,
    default_args,
    dataproc_zone="us-central1-a",
    dag_name="prio_staging",
    num_preemptible_workers=10,
):
    shared_config = {
        "cluster_name": "prio-staging",
        "gcp_conn_id": "google_cloud_prio_admin",
        "project_id": "moz-fx-prio-admin",
    }

    with DAG(
        "{}.{}".format(parent_dag_name, dag_name), default_args=default_args
    ) as dag:
        create_dataproc_cluster = DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            num_workers=2,
            image_version="1.4",
            zone=dataproc_zone,
            service_account="prio-admin-runner@moz-fx-prio-admin.iam.gserviceaccount.com",
            master_machine_type="n1-standard-8",
            worker_machine_type="n1-standard-8",
            num_preemptible_workers=num_preemptible_workers,
            metadata={"PIP_PACKAGES": "click"},
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            **shared_config
        )

        run_dataproc_spark = DataProcPySparkOperator(
            task_id="run_dataproc_spark",
            main="gs://moz-fx-data-prio-bootstrap/runner.py",
            pyfiles=["gs://moz-fx-data-prio-bootstrap/prio_processor.egg"],
            arguments=[
                "--date",
                "{{ ds }}",
                "--input",
                "gs://moz-fx-data-stage-data/telemetry-decoded_gcs-sink-doctype_prio/output",
                "--output",
                "gs://moz-fx-data-prio-data/staging/",
            ],
            **shared_config
        )

        delete_dataproc_cluster = DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster", trigger_rule="all_done", **shared_config
        )
        create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
        return dag


main_dag = DAG(
    dag_id="prio",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval="@weekly",
)


prio_staging_bootstrap = SubDagOperator(
    subdag=prio_processor(
        server_id="admin",
        cluster_name="gke-prio-admin",
        gcp_conn_id="google_cloud_prio_admin",
        service_account="prio-admin-runner@moz-fx-prio-admin.iam.gserviceaccount.com",
        arguments=[
            "bash",
            "-c",
            "cd processor; prio-processor bootstrap --output gs://moz-fx-data-prio-bootstrap",
        ],
        env_vars={},
    ),
    task_id="gke_prio_admin",
    dag=main_dag,
)

prio_staging = SubDagOperator(
    subdag=prio_staging(
        parent_dag_name=main_dag.dag_id,
        default_args=DEFAULT_ARGS,
        num_preemptible_workers=2,
    ),
    task_id="prio_staging",
    dag=main_dag,
)

# See: https://github.com/mozilla/prio-processor/blob/3cdc368707f8dc0f917d7b3d537c31645f4260f7/processor/tests/test_staging.py#L190-L205
copy_staging_data_to_server_a = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="copy_staging_data_to_server_a",
    source_bucket="moz-fx-data-prio-data",
    source_object="staging/submission_date={{ ds }}/server_id=a/*",
    destination_bucket="project-a-private",
    destination_object="raw/submission_date={{ ds }}/",
    google_cloud_storage_conn_id="google_cloud_prio_admin",
    dag=main_dag,
)

copy_staging_data_to_server_b = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="copy_staging_data_to_server_b",
    source_bucket="moz-fx-data-prio-data",
    source_object="staging/submission_date={{ ds }}/server_id=b/*",
    destination_bucket="project-b-private",
    destination_object="raw/submission_date={{ ds }}/",
    google_cloud_storage_conn_id="google_cloud_prio_admin",
    dag=main_dag,
)

prio_a = SubDagOperator(
    subdag=prio_processor(
        server_id="a",
        cluster_name="gke-prio-a",
        gcp_conn_id="google_cloud_prio_a",
        service_account="prio-runner-a@moz-fx-priotest-project-a.iam.gserviceaccount.com",
        arguments=["processor/bin/process"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config",
            "SERVER_ID": "A",
            "SHARED_SECRET": "m/AqDal/ZSA9597GwMM+VA==",
            # TODO: this is the built-in testing key
            "PRIVATE_KEY_HEX": "624BFDF22F729BBFD762B3D61930B876F3711B200A10F620FEAC6FD792A2BD08",
            "PUBLIC_KEY_HEX_INTERNAL": "AB0008BDE17581D3C45CA8CEACB3F7CE6FB48FEF98AA78597A6955633F54D628",
            "PUBLIC_KEY_HEX_EXTERNAL": "68499CBDCAE6B06CAC0C86D255A609B6AFF66A56087803CFE4BD998C7E20220C",
            "BUCKET_INTERNAL_PRIVATE": "project-a-private",
            "BUCKET_INTERNAL_SHARED": "project-a-shared",
            "BUCKET_EXTERNAL_SHARED": "project-b-shared",
            "RETRY_LIMIT": "5",
            "RETRY_DELAY": "3",
            "RETRY_BACKOFF_EXPONENT": "2",
        },
    ),
    # TODO: refactor to avoid hardcoding name here
    task_id="gke_prio_a",
    dag=main_dag,
)

prio_b = SubDagOperator(
    subdag=prio_processor(
        server_id="b",
        cluster_name="gke-prio-b",
        gcp_conn_id="google_cloud_prio_b",
        service_account="prio-runner-b@moz-fx-priotest-project-b.iam.gserviceaccount.com",
        arguments=["processor/bin/process"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config",
            "SERVER_ID": "B",
            "SHARED_SECRET": "m/AqDal/ZSA9597GwMM+VA==",
            "PRIVATE_KEY_HEX": "86EBA021A49C18B1D2885BCAE8C1985D14082F4A130F4862FD3E77DDD0518D3D",
            "PUBLIC_KEY_HEX_INTERNAL": "68499CBDCAE6B06CAC0C86D255A609B6AFF66A56087803CFE4BD998C7E20220C",
            "PUBLIC_KEY_HEX_EXTERNAL": "AB0008BDE17581D3C45CA8CEACB3F7CE6FB48FEF98AA78597A6955633F54D628",
            "BUCKET_INTERNAL_PRIVATE": "project-b-private",
            "BUCKET_INTERNAL_SHARED": "project-b-shared",
            "BUCKET_EXTERNAL_SHARED": "project-a-shared",
            "RETRY_LIMIT": "5",
            "RETRY_DELAY": "3",
            "RETRY_BACKOFF_EXPONENT": "2",
        },
    ),
    task_id="gke_prio_b",
    dag=main_dag,
)

prio_staging_bootstrap >> prio_staging
prio_staging >> copy_staging_data_to_server_a >> prio_a
prio_staging >> copy_staging_data_to_server_b >> prio_b
