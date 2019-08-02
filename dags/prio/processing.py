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


def prio_processor_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    server_id,
    gcp_conn_id,
    service_account,
    env_vars,
    arguments,
    *args,
    **kwargs
):
    assert server_id in ["a", "b", "admin"]

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    with DAG(
        "{}.{}".format(parent_dag_name, child_dag_name), default_args=default_args
    ) as dag:

        shared_config = {
            "project_id": connection.project_id,
            "location": "us-west1-b",
            "gcp_conn_id": gcp_conn_id,
            "cluster_name": "gke-prio-{}".format(server_id),
            "dag": dag,
        }

        create_gke_cluster = GKEClusterCreateOperator(
            task_id="create_gke_cluster",
            body=create_gke_config(
                name=shared_config["cluster_name"],
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
            task_id="processor_{}".format(server_id),
            name="run-prio-project-{}".format(server_id),
            namespace="default",
            image="mozilla/prio-processor:latest",
            arguments=arguments,
            env_vars=env_vars,
            **shared_config
        )

        delete_gke_cluster = GKEClusterDeleteOperator(
            task_id="delete_gke_cluster",
            name="delete_gke_cluster",
            trigger_rule="all_done",
            **shared_config
        )

        create_gke_cluster >> run_prio >> delete_gke_cluster
        return dag
