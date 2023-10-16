from airflow.models import DAG
from utils.gcp import bigquery_etl_query

GLAM_HISTOGRAM_AGGREGATES_FINAL_SUBDAG = "clients_histogram_aggregates"


def histogram_aggregates_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    schedule_interval,
    dataset_id,
    is_dev=False,
    docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
):
    GLAM_HISTOGRAM_AGGREGATES_SUBDAG = f"{parent_dag_name}.{child_dag_name}"
    default_args["depends_on_past"] = True
    dag = DAG(
        GLAM_HISTOGRAM_AGGREGATES_SUBDAG,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    clients_histogram_aggregates_new = bigquery_etl_query(
        reattach_on_restart=True,
        task_id="clients_histogram_aggregates_new",
        destination_table="clients_histogram_aggregates_new_v1",
        dataset_id=dataset_id,
        project_id="moz-fx-data-shared-prod",
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
    return dag
