from airflow.models import DAG

from utils.gcp import bigquery_etl_query


def merge_params(min_param, max_param, additional_params):
    parameters = (
        f"min_sample_id:INT64:{min_param}",
        f"max_sample_id:INT64:{max_param}",
    )

    if additional_params is not None:
        parameters += additional_params

    return parameters


def repeated_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    schedule_interval,
    dataset_id,
    additional_params=None,
    num_partitions=5,
    date_partition_parameter="submission_date",
):
    dag = DAG(
        f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args,
        schedule_interval=schedule_interval,
        concurrency=1,
    )

    # This task runs first and replaces the relevant partition, followed
    # by the next tasks that append to the same partition of the same table.
    NUM_SAMPLE_IDS = 100
    PARTITION_SIZE = NUM_SAMPLE_IDS // num_partitions

    if NUM_SAMPLE_IDS % num_partitions != 0:
        raise ValueError(f"Number of partitions must be a divisor "
                         f"of the number of sample ids ({NUM_SAMPLE_IDS})")

    task_0 = bigquery_etl_query(
        task_id=f"{child_dag_name}_0",
        destination_table=f"{child_dag_name}_v1",
        dataset_id=dataset_id,
        project_id="moz-fx-data-shared-prod",
        owner="msamuel@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
        depends_on_past=True,
        parameters=merge_params(0, PARTITION_SIZE - 1, additional_params),
        date_partition_parameter=date_partition_parameter,
        arguments=("--replace",),
        dag=dag,
    )

    for partition in range(1, num_partitions):
        min_param = partition * PARTITION_SIZE
        max_param = min_param + PARTITION_SIZE - 1

        task = bigquery_etl_query(
            task_id=f"{child_dag_name}_{partition}",
            destination_table=f"{child_dag_name}_v1",
            dataset_id=dataset_id,
            project_id="moz-fx-data-shared-prod",
            owner="msamuel@mozilla.com",
            email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
            depends_on_past=True,
            parameters=merge_params(min_param, max_param, additional_params),
            date_partition_parameter=date_partition_parameter,
            arguments=("--append_table", "--noreplace",),
            dag=dag,
        )
        task_0 >> task

    return dag
