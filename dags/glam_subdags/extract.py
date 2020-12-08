from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_delete_operator import (
    GoogleCloudStorageDeleteOperator,
)
from airflow.executors import get_default_executor
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from utils.gcp import bigquery_etl_query


gcp_conn = GoogleCloudBaseHook("google_cloud_airflow_dataproc")
project_id = "moz-fx-data-shared-prod"
glam_bucket = "moz-fx-data-glam-prod-fca7-etl-data"


def extracts_subdag(
    parent_dag_name, child_dag_name, default_args, schedule_interval, dataset_id
):
    dag_id = "{}.{}".format(parent_dag_name, child_dag_name)
    dag = DAG(
        dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval
    )

    for channel in ("nightly", "beta", "release"):
        SubDagOperator(
            subdag=extract_channel_subdag(
                dag_id,
                "extract_{}".format(channel),
                default_args,
                schedule_interval,
                dataset_id,
                channel,
            ),
            task_id="extract_{}".format(channel),
            executor=get_default_executor(),
            dag=dag,
        )

    return dag


def extract_channel_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    schedule_interval,
    dataset_id,
    channel,
):
    dag = DAG(
        dag_id="{}.{}".format(parent_dag_name, child_dag_name),
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    bq_extract_table = "glam_extract_firefox_{}_v1".format(channel)
    etl_query = bigquery_etl_query(
        task_id="glam_client_probe_counts_{}_extract".format(channel),
        destination_table=bq_extract_table,
        dataset_id=dataset_id,
        project_id=project_id,
        owner="robhudson@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "msamuel@mozilla.com",
            "robhudson@mozilla.com",
        ],
        date_partition_parameter=None,
        arguments=("--replace",),
        sql_file_path="sql/moz-fx-data-shared-prod/{}/glam_client_probe_counts_extract_v1/query.sql".format(
            dataset_id
        ),
        parameters=("channel:STRING:{}".format(channel),),
        dag=dag,
    )

    gcs_delete = GoogleCloudStorageDeleteOperator(
        task_id="glam_gcs_delete_old_{}_extracts".format(channel),
        bucket_name=glam_bucket,
        prefix="aggs-desktop-{}".format(channel),
        google_cloud_storage_conn_id=gcp_conn.gcp_conn_id,
        dag=dag,
    )

    gcs_destination = "gs://{bucket}/aggs-desktop-{channel}-*.csv".format(
        bucket=glam_bucket, channel=channel
    )
    bq2gcs = BigQueryToCloudStorageOperator(
        task_id="glam_extract_{}_to_csv".format(channel),
        source_project_dataset_table="{}.{}.{}".format(
            project_id, dataset_id, bq_extract_table
        ),
        destination_cloud_storage_uris=gcs_destination,
        bigquery_conn_id=gcp_conn.gcp_conn_id,
        export_format="CSV",
        print_header=False,
        dag=dag,
    )

    etl_query >> gcs_delete >> bq2gcs

    return dag


def extract_user_counts(
    parent_dag_name,
    child_dag_name,
    default_args,
    schedule_interval,
    dataset_id
):

    dag = DAG(
        dag_id="{}.{}".format(parent_dag_name, child_dag_name),
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    bq_extract_table = "glam_user_counts_extract_v1"
    etl_query = bigquery_etl_query(
        task_id="glam_user_counts_extract",
        destination_table=bq_extract_table,
        dataset_id=dataset_id,
        project_id=project_id,
        owner="robhudson@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "msamuel@mozilla.com",
            "robhudson@mozilla.com",
        ],
        date_partition_parameter=None,
        arguments=("--replace",),
        dag=dag,
    )

    gcs_delete = GoogleCloudStorageDeleteOperator(
        task_id="glam_gcs_delete_count_extracts",
        bucket_name=glam_bucket,
        prefix="glam-extract-firefox-counts",
        google_cloud_storage_conn_id=gcp_conn.gcp_conn_id,
        dag=dag,
    )

    gcs_destination = "gs://{}/glam-extract-firefox-counts.csv".format(
        glam_bucket
    )
    bq2gcs = BigQueryToCloudStorageOperator(
        task_id="glam_extract_user_counts_to_csv",
        source_project_dataset_table="{}.{}.{}".format(
            project_id, dataset_id, bq_extract_table
        ),
        destination_cloud_storage_uris=gcs_destination,
        bigquery_conn_id=gcp_conn.gcp_conn_id,
        export_format="CSV",
        print_header=False,
        dag=dag,
    )

    etl_query >> gcs_delete >> bq2gcs

    return dag
