from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.s3_to_gcs_transfer_operator import (
    S3ToGoogleCloudStorageTransferOperator,
)
from operators.gcp_container_operator import (
    GKEClusterCreateOperator,
    GKEClusterDeleteOperator,
    GKEPodOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
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


def create_prio_dag(
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
        trigger_rule="one_failed",
        **shared_config
    )

    create_gke_cluster >> run_prio >> delete_gke_cluster
    return gke_dag


main_dag = DAG(
    dag_id="prio",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    schedule_interval="@weekly",
)


admin_bootstrap = SubDagOperator(
    subdag=create_prio_dag(
        server_id="admin",
        cluster_name="gke-prio-admin",
        gcp_conn_id="google_cloud_prio_admin",
        service_account="prio-admin-runner@moz-fx-prio-admin.iam.gserviceaccount.com",
        arguments=[
            "bash",
            "-c",
            "cd processor; prio-processor --output gs://moz-fx-data-prio-bootstrap",
        ],
        env_vars={},
    ),
    task_id="gke_prio_admin",
    dag=main_dag,
)

prio_a = SubDagOperator(
    subdag=create_prio_dag(
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
    subdag=create_prio_dag(
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

admin_bootstrap >> prio_a
admin_bootstrap >> prio_b
