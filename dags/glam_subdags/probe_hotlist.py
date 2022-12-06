from utils.gcp import gke_command


def update_hotlist(
    task_id,
    project_id,
    source_dataset_id,
    destination_dataset_id=None,
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    gcp_conn_id="google_cloud_airflow_gke",
    **kwargs,
):
    """
    :param task_id:                     Airflow task id
    :param project_id:                  GCP project to write to
    :param source_dataset_id:           Bigquery dataset to read from in queries
    :param destination_dataset_id:      Bigquery dataset to write results to.  Defaults to source_dataset_id
    :param docker_image:                Docker image
    :param gcp_conn_id:                 Airflow GCP connection
    """
    if destination_dataset_id is None:
        destination_dataset_id = source_dataset_id
    env_vars = {
        "PROJECT": project_id,
        "PROD_DATASET": source_dataset_id,
        "DATASET": destination_dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
    }
    command = [
        "script/glam/update_probe_hotlist"
    ]
    return gke_command(
        task_id=task_id,
        cmds=["bash"],
        env_vars=env_vars,
        command=command,
        docker_image=docker_image,
        gcp_conn_id=gcp_conn_id,
        **kwargs,
    )
