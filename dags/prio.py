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
from utils.gke import create_gke_config

DEFAULT_ARGS = {
    "owner": "hwoo@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 24),
    "email": ["hwoo@mozilla.com", "dataops+alerts@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


def create_prio_dag(
    server_id, cluster_name, gcp_conn_id, service_account, *args, **kwargs
):
    assert server_id in ["a", "b"]

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    gke_dag = DAG(
        "gke_prio_{}".format(server_id),
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
        arguments=["scripts/test-cli-integration"],
        **shared_config
    )

    delete_gke_cluster = GKEClusterDeleteOperator(
        task_id="delete_gke_cluster", name=cluster_name, **shared_config
    )

    create_gke_cluster >> run_prio >> delete_gke_cluster
    return gke_dag


prio_a = create_prio_dag(
    server_id="a",
    cluster_name="gke-prio-a",
    gcp_conn_id="google_cloud_prio_a",
    service_account="prio-runner-a@moz-fx-priotest-project-a.iam.gserviceaccount.com",
)

prio_b = create_prio_dag(
    server_id="b",
    cluster_name="gke-prio-b",
    gcp_conn_id="google_cloud_prio_b",
    service_account="prio-runner-a@moz-fx-priotest-project-b.iam.gserviceaccount.com",
)
