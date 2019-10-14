from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

def export(
    leanplum_app_id,
    leanplum_client_key,
    gcs_bucket,
    bq_dataset_id,
    task_id,
    gcs_prefix=None,
    project_id=None,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="mozilla/leanplum-data-export:latest",
    image_pull_policy="Always",
    **kwargs
):
    """ Export a day of data from Leanplum for a single application,
        and make it available in BigQuery.

    :param str leanplum_app_id:      [Required] Leanplum application ID
    :param str leanplum_client_key:  [Required] Leanplum client key
    :param str gcs_bucket:           [Required] GCS Bucket to export data to
    :param str bq_dataset:           [Required] BigQuery default dataset id
    :param str task_id:              [Required] The task ID for this task
    :param str gcs_prefix:           Prefix for data exported to GCS
    :param str project_id:           Project the GKE cluster is in
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
    if project_id is None:
        project_id = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id

    if gcs_prefix is None:
        gcs_prefix = ""

    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        image_pull_policy=image_pull_policy,
        arguments=["leanplum-data-export"]
            + ["export-leanplum"]
            + ["--app-id", leanplum_app_id]
            + ["--client-key", leanplum_client_key]
            + ["--date", "{{ ds_nodash }}"]
            + ["--bucket", gcs_bucket]
            + ["--prefix", gcs_prefix]
            + ["--bq-dataset", bq_dataset_id],
        **kwargs)
