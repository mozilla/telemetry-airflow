from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator

from glam_subdags.general import repeated_subdag
from utils.gcp import bigquery_etl_query


GLAM_HISTOGRAM_AGGREGATES_FINAL_SUBDAG = "clients_histogram_aggregates"


def histogram_aggregates_subdag(
    parent_dag_name, child_dag_name, default_args, schedule_interval, dataset_id
):
    GLAM_HISTOGRAM_AGGREGATES_SUBDAG = "%s.%s" % (parent_dag_name, child_dag_name)
    default_args["depends_on_past"] = True
    dag = DAG(
        GLAM_HISTOGRAM_AGGREGATES_SUBDAG,
        default_args=default_args,
        schedule_interval=schedule_interval,
        concurrency=1,
    )

    clients_histogram_aggregates_new = bigquery_etl_query(
        task_id="clients_histogram_aggregates_new",
        destination_table="clients_histogram_aggregates_new_v1",
        dataset_id=dataset_id,
        project_id="moz-fx-data-shared-prod",
        owner="msamuel@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
        date_partition_parameter=None,
        parameters=("submission_date:DATE:{{ds}}",),
        arguments=("--replace",),
        dag=dag,
    )

    clients_histogram_aggregates_final = SubDagOperator(
        subdag=repeated_subdag(
            GLAM_HISTOGRAM_AGGREGATES_SUBDAG,
            GLAM_HISTOGRAM_AGGREGATES_FINAL_SUBDAG,
            default_args,
            dag.schedule_interval,
            dataset_id,
        ),
        task_id=GLAM_HISTOGRAM_AGGREGATES_FINAL_SUBDAG,
        dag=dag,
    )

    clients_histogram_aggregates_new >> clients_histogram_aggregates_final
    return dag
