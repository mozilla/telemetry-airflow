from datetime import timedelta
from os import environ

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.gcp_container_operator import (
    GKEClusterCreateOperator,
    GKEClusterDeleteOperator,
)
from operators.gcp_container_operator import GKEPodOperator

from utils.gke import create_gke_config
from airflow.operators.bash_operator import BashOperator


def container_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    gcp_conn_id,
    service_account,
    server_id,
    env_vars={},
    arguments=[],
    machine_type="n1-standard-1",
    image="mozilla/prio-processor:v1.6.1",
    location="us-west1-b",
    owner_label="amiyaguchi",
    team_label="dataeng",
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
    :param str machine_type:            The machine type for running the image.
    :param str image:                   Dockerhub image
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
                machine_type=machine_type,
                # DataProc clusters require VPC with auto-created subnets
                subnetwork="default" if server_id == "admin" else "gke-subnet",
                is_dev=environ.get("DEPLOY_ENVIRONMENT") == "dev",
            ),
            dag=dag,
            **shared_config
        )

        # Running the pod without any time in-between will cause the scope-based
        # authentication in Google Cloud Platform to fail. For example:
        #
        # `ServiceException: 401 Anonymous caller does not have
        # storage.objects.get access to moz-fx-prio-dev-a-private/processed/`
        #
        # Sleeping by a small amount solves this problem. This issue was first
        # noticed intermittently on 2019-09-09.
        sleep = BashOperator(task_id="sleep", bash_command="sleep 60", dag=dag)

        run_prio = GKEPodOperator(
            task_id="processor_{}".format(server_id),
            name="run-prio-project-{}".format(server_id),
            cluster_name=cluster_name,
            namespace="default",
            image=image,
            arguments=arguments,
            env_vars=env_vars,
            dag=dag,
            **shared_config
        )

        delete_gke_cluster = GKEClusterDeleteOperator(
            task_id="delete_gke_cluster",
            name=cluster_name,
            trigger_rule="all_done",
            dag=dag,
            **shared_config
        )

        create_gke_cluster >> sleep >> run_prio >> delete_gke_cluster
        return dag
