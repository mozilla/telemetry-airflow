from utils.gcp import gke_command


def export(
    bq_dataset_id,
    task_id,
    bq_project,
    s3_prefix,
    version,
    s3_bucket="moz-fx-data-us-west-2-leanplum-export",
    gcs_bucket="moz-fx-data-prod-external-data",
    table_prefix=None,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/leanplum-data-export:latest",
    aws_conn_id="aws_data_iam_s3",
    **kwargs
):
    """ Export a day of data from Leanplum for a single application,
        and make it available in BigQuery.

    See bug 1588654 for information on which buckets and datasets
    these tabes should live in.

    :param str bq_dataset_id:           [Required] BigQuery default dataset id
    :param str task_id:              [Required] The task ID for this task
    :param str bq_project:           [Required] The project to create tables in
    :param str s3_prefix:            Prefix for data in the s3 bucket
    :param str s3_bucket:            [Required] S3 bucket to retrieve streaming exports from
    :param str gcs_bucket:           GCS Bucket to export data to
    :param str table_prefix:         Prefix of tables in Bigquery
    :param str version:              Version of the destination table
    :param str gcp_conn_id:          Airflow connection id for GCP access
    :param str gke_location:         GKE cluster location
    :param str gke_cluster_name:     GKE cluster name
    :param str gke_namespace:        GKE cluster namespace
    :param str docker_image:         docker image to use
    :param str aws_conn_id:          Airflow connection id for AWS access
    :param Dict[str, Any] kwargs:    Additional keyword arguments for
                                     GKEPodOperator

    :return: GKEPodOperator
    """
    args = ["leanplum-data-export",
            "export-leanplum",
            "--date", "{{ ds_nodash }}",
            "--bucket", gcs_bucket,
            "--bq-dataset", bq_dataset_id,
            "--project", bq_project,
            "--s3-bucket", s3_bucket,
            "--version", version,
            "--prefix",  s3_prefix]

    if table_prefix is not None:
        args += ["--table-prefix", table_prefix]

    return gke_command(
        task_id=task_id,
        docker_image=docker_image,
        command=args,
        gcp_conn_id=gcp_conn_id,
        gke_location=gke_location,
        gke_cluster_name=gke_cluster_name,
        gke_namespace=gke_namespace,
        aws_conn_id=aws_conn_id,
        **kwargs)
