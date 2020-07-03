from utils.gcp import gke_command


def generate_and_run_query(task_id,
                           project_id,
                           dataset_id,
                           sample_size,
                           append_results,
                           probe_type,
                           process=None,
                           docker_image="mozilla/bigquery-etl:latest",
                           gcp_conn_id="google_cloud_derived_datasets",
                           **kwargs):
    env_vars = {
        "PROJECT": project_id,
        "PROD_DATASET": dataset_id,
        "DATASET": dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        "RUN_QUERY": "t",
    }
    if append_results:
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
        **kwargs,
    )