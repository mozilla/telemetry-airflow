from datetime import timedelta
from os import environ

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import (
    GKEClusterCreateOperator,
    GKEClusterDeleteOperator,
    GKEPodOperator,
)
from utils.gke import create_gke_config


def create_prio_processor(
    default_args,
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
        default_args=default_args,
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
