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
    gcp_conn_id,
    service_account,
    server_id,
    env_vars,
    arguments,
    location="us-west1-b",
    owner_label="hwoo",
    team_label="dataops",
):
    """Run a command on an ephemeral container running the
    `mozilla/prio-processor:latest` image.

    :param str parent_dag_name:         Name of the parent DAG.
    :param str child_dag_name:          Name of the child DAG.
    :param Dict[str, Any] default_args: Default arguments for the child DAG.
    :param str gcp_conn_id:             Name of the connection string.
    :param str service_account:         The address of the service account.
    :param str server_id:               The identifier for the Prio processor
    :param Dict[str, str] env_vars:     Environment variables for configuring
                                        the processor.
    :param List[str] arguments:         The command to run after loading the
                                        image.
    :param str location:                The region of the GKE cluster.
    :param str owner_label:             Label for associating the owner
    :param str team_label:              Label for associating the team
    :return: DAG
    """
    assert server_id in ["a", "b", "admin"]

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    cluster_name = "gke-prio-{}".format(server_id)

    shared_config = {
        "project_id": connection.project_id,
        "gcp_conn_id": gcp_conn_id,
        "location": location,
    }

    with DAG(
        "{}.{}".format(parent_dag_name, child_dag_name), default_args=default_args
    ) as dag:
        create_gke_cluster = GKEClusterCreateOperator(
            task_id="create_gke_cluster",
            body=create_gke_config(
                name=cluster_name,
                service_account=service_account,
                owner_label=owner_label,
                team_label=team_label,
                # DataProc clusters require VPC with auto-created subnets
                subnetwork="default" if server_id == "admin" else "gke-subnet",
                is_dev=environ.get("DEPLOY_ENVIRONMENT") == "dev",
            ),
            **shared_config
        )

        run_prio = GKEPodOperator(
            task_id="processor_{}".format(server_id),
            name="run-prio-project-{}".format(server_id),
            cluster_name=cluster_name,
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
        return dag
