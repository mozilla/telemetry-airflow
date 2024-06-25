from airflow.models import DAG
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

from utils.gcp import bigquery_etl_query

gcp_conn_id = "google_cloud_airflow_dataproc"
project_id = "moz-fx-data-shared-prod"
glam_bucket = "moz-fx-data-glam-prod-fca7-etl-data"


def extract_user_counts(
    parent_dag_name,
    child_dag_name,
    default_args,
    schedule_interval,
    table_project_id,
    billing_project_id,
    fully_qualified_dataset,
    dataset_id,
    task_prefix,
    file_prefix,
):
    bq_extract_table = f"glam_{task_prefix}_extract_v1"
    dag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    etl_query = bigquery_etl_query(
        task_id=f"glam_{task_prefix}_extract",
        destination_table=bq_extract_table,
        dataset_id=fully_qualified_dataset,
        sql_file_path=f"sql/{table_project_id}/{dataset_id}/{bq_extract_table}/query.sql",
        project_id=billing_project_id,
        date_partition_parameter=None,
        arguments=("--replace",),
        dag=dag,
    )

    gcs_delete = GCSDeleteObjectsOperator(
        task_id=f"glam_gcs_delete_{task_prefix}_extracts",
        bucket_name=glam_bucket,
        prefix=f"glam-extract-firefox-{file_prefix}",
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )

    if file_prefix == "sample-counts":
        gcs_destination = f"gs://{glam_bucket}/glam-extract-firefox-{file_prefix}-*.csv"
    else:
        gcs_destination = f"gs://{glam_bucket}/glam-extract-firefox-{file_prefix}.csv"

    bq2gcs = BigQueryToGCSOperator(
        task_id=f"glam_extract_{task_prefix}_to_csv",
        source_project_dataset_table=f"{project_id}.{dataset_id}.{bq_extract_table}",
        destination_cloud_storage_uris=gcs_destination,
        gcp_conn_id=gcp_conn_id,
        export_format="CSV",
        print_header=False,
        dag=dag,
    )

    etl_query >> gcs_delete >> bq2gcs

    return dag
