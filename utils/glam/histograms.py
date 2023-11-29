from airflow.utils.task_group import TaskGroup

from utils.gcp import bigquery_etl_query


def histogram_aggregates_task_group(
    task_group_name,
    dag,
    default_args,
    dataset_id,
    is_dev=False,
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
):
    with TaskGroup(task_group_name, dag=dag, default_args=default_args) as task_group:
        clients_histogram_aggregates_new = bigquery_etl_query(
            reattach_on_restart=True,
            task_id="clients_histogram_aggregates_new",
            destination_table="clients_histogram_aggregates_new_v1",
            dataset_id=dataset_id,
            project_id="moz-fx-data-shared-prod",
            depends_on_past=True,
            date_partition_parameter=None,
            parameters=("submission_date:DATE:{{ds}}",),
            arguments=("--replace",),
            dag=dag,
            docker_image=docker_image,
        )

        clients_histogram_aggregates_final = bigquery_etl_query(
            reattach_on_restart=True,
            task_id="clients_histogram_aggregates_v2",
            destination_table="clients_histogram_aggregates_v2",
            dataset_id=dataset_id,
            project_id="moz-fx-data-shared-prod",
            depends_on_past=True,
            parameters=("submission_date:DATE:{{ds}}",),
            date_partition_parameter=None,
            arguments=("--replace",),
            dag=dag,
            docker_image=docker_image,
        )

        clients_histogram_aggregates_new >> clients_histogram_aggregates_final

    return task_group
