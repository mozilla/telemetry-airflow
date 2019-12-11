from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

def simpleprophet_forecast(
    task_id,
    datasource,
    project_id,
    dataset_id,
    table_id,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="gcr.io/moz-fx-data-forecasting/simpleprophet:latest",
    image_pull_policy="Always",
    **kwargs
):
    """Run all simpleprophet models for the given datasource and model date.

    :param str task_id:              [Required] ID for the task
    :param str datasource:           [Required] One of desktop, mobile, fxa
    :param str project_id:           [Required] ID of project where target table lives
    :param str dataset_id:           [Required] ID of dataset where target table lives
    :param str table_id:             [Required] ID of target table

    :param str gcp_conn_id:          Airflow connection id for GCP access
    :param str gke_location:         GKE cluster location
    :param str gke_cluster_name:     GKE cluster name
    :param str gke_namespace:        GKE cluster namespace
    :param str docker_image:         docker image to use
    :param str image_pull_policy:    Kubernetes policy for when to pull
                                     docker_image
    :param Dict[str, Any] kwargs:    Additional keyword arguments for
                                     GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))
    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=["{{ds}}"]
        + ["--datasource=" + datasource]
        + ["--project-id=" + project_id]
        + ["--dataset-id=" + dataset_id]
        + ["--table-id=" + table_id],
        image_pull_policy=image_pull_policy,
        **kwargs
    )
