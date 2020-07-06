from utils.gcp import gke_command


def generate_and_run_query(task_id,
                           project_id,
                           source_dataset_id,
                           sample_size,
                           overwrite,
                           probe_type,
                           destination_dataset_id=None,
                           process=None,
                           docker_image="mozilla/bigquery-etl:latest",
                           gcp_conn_id="google_cloud_derived_datasets",
                           **kwargs):
    """
    :param task_id:                     Airflow task id
    :param project_id:                  GCP project to write to
    :param source_dataset_id:           Bigquery dataset to read from in queries
    :param sample_size:                 Value to use for windows release client sampling
    :param overwrite:                   Overwrite the destination table
    :param probe_type:                  Probe type to generate query
    :param destination_dataset_id:      Bigquery dataset to write results to.  Defaults to source_dataset_id
    :param process:                     Process to filter probes for.  Gets all processes by default.
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
        "RUN_QUERY": "t",
    }
    if not overwrite:
        env_vars["APPEND"] = "t"

    command = [
        "script/glam/generate_and_run_desktop_sql",
        probe_type,
        sample_size,
    ]
    if process is not None:
        command.append(process)

    return gke_command(
        task_id=task_id,
        cmds=["bash"],
        env_vars=env_vars,
        command=command,
        docker_image=docker_image,
        gcp_conn_id=gcp_conn_id,
        **kwargs
    )
