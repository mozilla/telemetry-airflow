from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from utils.gcp import bigquery_etl_query


gcp_conn_id = "google_cloud_airflow_dataproc"
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
        owner="akommasani@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "akommasani@mozilla.com",
            "linhnguyen@mozilla.com",
        ],
        date_partition_parameter=None,
        arguments=("--replace",),
        sql_file_path="sql/moz-fx-data-shared-prod/{}/glam_client_probe_counts_extract_v1/query.sql".format(
            dataset_id
        ),
        parameters=("channel:STRING:{}".format(channel),),
        dag=dag,
    )

    gcs_delete = GCSDeleteObjectsOperator(
        task_id="glam_gcs_delete_old_{}_extracts".format(channel),
        bucket_name=glam_bucket,
        prefix="aggs-desktop-{}".format(channel),
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )

    gcs_destination = "gs://{bucket}/aggs-desktop-{channel}-*.csv".format(
        bucket=glam_bucket, channel=channel
    )
    bq2gcs = BigQueryToGCSOperator(
        task_id="glam_extract_{}_to_csv".format(channel),
        source_project_dataset_table="{}.{}.{}".format(
            project_id, dataset_id, bq_extract_table
        ),
        destination_cloud_storage_uris=gcs_destination,
        gcp_conn_id=gcp_conn_id,
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
    dataset_id,
    task_prefix,
    file_prefix
):
    bq_extract_table="glam_{}_extract_v1".format(task_prefix)
    dag = DAG(
        dag_id="{}.{}".format(parent_dag_name, child_dag_name),
        default_args=default_args,
        schedule_interval=schedule_interval,
    )
    
    etl_query = bigquery_etl_query(
        task_id="glam_{}_extract".format(task_prefix),
        destination_table=bq_extract_table,
        dataset_id=dataset_id,
        project_id=project_id,
        owner="akommasani@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "akommasani@mozilla.com",
            "linhnguyen@mozilla.com",
        ],
        date_partition_parameter=None,
        arguments=("--replace",),
        dag=dag,
    )


    gcs_delete = GCSDeleteObjectsOperator(
        task_id="glam_gcs_delete_{}_extracts".format(task_prefix),
        bucket_name=glam_bucket,

        prefix="glam-extract-firefox-{}".format(file_prefix),
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )

    if file_prefix=="sample-counts":
        gcs_destination = "gs://{}/glam-extract-firefox-{}-*.csv".format(
        glam_bucket, file_prefix
    )
    else: 
        gcs_destination = "gs://{}/glam-extract-firefox-{}.csv".format(
        glam_bucket, file_prefix
    )

    bq2gcs = BigQueryToGCSOperator(
        task_id="glam_extract_{}_to_csv".format(task_prefix),
        source_project_dataset_table="{}.{}.{}".format(
            project_id, dataset_id, bq_extract_table
        ),
        destination_cloud_storage_uris=gcs_destination,
        gcp_conn_id=gcp_conn_id,
        export_format="CSV",
        print_header=False,
        dag=dag,
    )

    etl_query >> gcs_delete >> bq2gcs

    return dag
