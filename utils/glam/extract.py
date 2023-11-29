from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.utils.task_group import TaskGroup

from utils.gcp import bigquery_etl_query

gcp_conn_id = "google_cloud_airflow_dataproc"
project_id = "moz-fx-data-shared-prod"
glam_bucket = "moz-fx-data-glam-prod-fca7-etl-data"


def extracts_task_group(task_group_name, dag, default_args, dataset_id):
    with TaskGroup(task_group_name, dag=dag, default_args=default_args) as task_group:
        for channel in ("nightly", "beta", "release"):
            extract_channel_task_group(
                f"extract_{channel}",
                dag,
                default_args,
                dataset_id,
                channel,
            )

    return task_group


def extract_channel_task_group(task_group_name, dag, default_args, dataset_id, channel):
    bq_extract_table = f"glam_extract_firefox_{channel}_v1"

    with TaskGroup(task_group_name, dag=dag, default_args=default_args) as task_group:
        etl_query = bigquery_etl_query(
            task_id=f"glam_client_probe_counts_{channel}_extract",
            destination_table=bq_extract_table,
            dataset_id=dataset_id,
            project_id=project_id,
            date_partition_parameter=None,
            arguments=("--replace",),
            sql_file_path="sql/moz-fx-data-shared-prod/{}/glam_client_probe_counts_extract_v1/query.sql".format(
                dataset_id
            ),
            parameters=(f"channel:STRING:{channel}",),
        )

        gcs_delete = GCSDeleteObjectsOperator(
            task_id=f"glam_gcs_delete_old_{channel}_extracts",
            bucket_name=glam_bucket,
            prefix=f"aggs-desktop-{channel}",
            gcp_conn_id=gcp_conn_id,
        )

        gcs_destination = "gs://{bucket}/aggs-desktop-{channel}-*.csv".format(
            bucket=glam_bucket, channel=channel
        )
        bq2gcs = BigQueryToGCSOperator(
            task_id=f"glam_extract_{channel}_to_csv",
            source_project_dataset_table="{}.{}.{}".format(
                project_id, dataset_id, bq_extract_table
            ),
            destination_cloud_storage_uris=gcs_destination,
            gcp_conn_id=gcp_conn_id,
            export_format="CSV",
            print_header=False,
        )

        etl_query >> gcs_delete >> bq2gcs

    return task_group


def extract_user_counts(
    task_group_name,
    dag,
    default_args,
    dataset_id,
    task_prefix,
    file_prefix,
):
    bq_extract_table = f"glam_{task_prefix}_extract_v1"

    with TaskGroup(task_group_name, dag=dag, default_args=default_args) as task_group:
        etl_query = bigquery_etl_query(
            task_id=f"glam_{task_prefix}_extract",
            destination_table=bq_extract_table,
            dataset_id=dataset_id,
            project_id=project_id,
            date_partition_parameter=None,
            arguments=("--replace",),
        )

        gcs_delete = GCSDeleteObjectsOperator(
            task_id=f"glam_gcs_delete_{task_prefix}_extracts",
            bucket_name=glam_bucket,
            prefix=f"glam-extract-firefox-{file_prefix}",
            gcp_conn_id=gcp_conn_id,
        )

        if file_prefix == "sample-counts":
            gcs_destination = "gs://{}/glam-extract-firefox-{}-*.csv".format(
                glam_bucket, file_prefix
            )
        else:
            gcs_destination = "gs://{}/glam-extract-firefox-{}.csv".format(
                glam_bucket, file_prefix
            )

        bq2gcs = BigQueryToGCSOperator(
            task_id=f"glam_extract_{task_prefix}_to_csv",
            source_project_dataset_table="{}.{}.{}".format(
                project_id, dataset_id, bq_extract_table
            ),
            destination_cloud_storage_uris=gcs_destination,
            gcp_conn_id=gcp_conn_id,
            export_format="CSV",
            print_header=False,
        )

        etl_query >> gcs_delete >> bq2gcs

    return task_group
