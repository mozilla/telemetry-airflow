from operators.gcp_container_operator import GKEPodOperator


def generate_and_run_desktop_query(
    task_id,
    project_id,
    billing_project_id,
    source_dataset_id,
    sample_size,
    overwrite,
    probe_type,
    reattach_on_restart=False,
    destination_dataset_id=None,
    process=None,
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    **kwargs,
):
    """
    Generate and run firefox desktop queries.

    :param task_id:                     Airflow task id
    :param project_id:                  GCP project to write to
    :param billing_project_id:          Project to run query on and used for billing
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
        "BILLING_PROJECT": billing_project_id,
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

    return GKEPodOperator(
        reattach_on_restart=reattach_on_restart,
        task_id=task_id,
        cmds=["bash"],
        env_vars=env_vars,
        arguments=command,
        image=docker_image,
        is_delete_operator_pod=False,
        **kwargs,
    )


def generate_and_run_glean_queries(
    task_id,
    product,
    destination_project_id,
    destination_dataset_id="glam_etl",
    source_project_id="moz-fx-data-shared-prod",
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    env_vars=None,
    **kwargs,
):
    """
    Generate and run fog and fenix queries.

    :param task_id:                     Airflow task id
    :param product:                     Product name of glean app
    :param destination_project_id:      Project to store derived tables
    :param destination_dataset_id:      Name of the dataset to store derived tables
    :param source_project_id:           Project containing the source datasets
    :param docker_image:                Docker image
    :param gcp_conn_id:                 Airflow GCP connection
    :param env_vars:                    Additional environment variables to pass
    """
    if env_vars is None:
        env_vars = {}

    env_vars = {
        "PRODUCT": product,
        "SRC_PROJECT": source_project_id,
        "PROJECT": destination_project_id,
        "DATASET": destination_dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        **env_vars,
    }

    return GKEPodOperator(
        reattach_on_restart=True,
        task_id=task_id,
        cmds=["bash", "-c"],
        env_vars=env_vars,
        arguments=["script/glam/generate_glean_sql && script/glam/run_glam_sql"],
        image=docker_image,
        is_delete_operator_pod=False,
        **kwargs,
    )


def generate_and_run_glean_task(
    task_type,
    task_name,
    product,
    destination_project_id,
    destination_dataset_id="glam_etl",
    source_project_id="moz-fx-data-shared-prod",
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    env_vars=None,
    min_sample_id = 0,
    max_sample_id = 99,
    replace_table = True,
    **kwargs,
):
    """
    See https://github.com/mozilla/bigquery-etl/blob/main/script/glam/run_glam_sql.

    :param task_type:                   Either view, init, or query
    :param task_name:                   Name of the task, derives the name of the query
    :param product:                     Product name of glean app
    :param destination_project_id:      Project to store derived tables
    :param destination_dataset_id:      Name of the dataset to store derived tables
    :param source_project_id:           Project containing the source datasets
    :param docker_image:                Docker image
    :param gcp_conn_id:                 Airflow GCP connection
    :param env_vars:                    Additional environment variables to pass
    """
    if env_vars is None:
        env_vars = {}

    query_name = task_name.split("_sampled_")[0]
    # If a range smaller than 100% of the samples is being used then sample_id is needed.
    use_sample_id = min_sample_id > 0 or max_sample_id < 99

    write_preference = "--replace" if replace_table else "'--append_table --noreplace'"

    env_vars = {
        "PRODUCT": product,
        "SRC_PROJECT": source_project_id,
        "PROJECT": destination_project_id,
        "DATASET": destination_dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        "IMPORT": "true",
        "USE_SAMPLE_ID": str(use_sample_id),
        **env_vars,
    }
    if task_type not in ["view", "init", "query"]:
        raise ValueError("task_type must be either a view, init, or query")

    return GKEPodOperator(
        reattach_on_restart=True,
        task_id=f"{task_type}_{task_name}",
        cmds=["bash", "-c"],
        env_vars=env_vars,
        arguments=[
            "script/glam/generate_glean_sql && "
            "source script/glam/run_glam_sql && "
            f'run_{task_type} {query_name} false {min_sample_id} {max_sample_id} "" {write_preference}'
        ],
        image=docker_image,
        is_delete_operator_pod=False,
        **kwargs,
    )
