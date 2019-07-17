import datetime

from airflow import models
from utils.gcp import bigquery_etl_query
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

default_args = {
    'owner': 'frank@mozilla.com',
    'start_date': datetime.datetime(2019, 6, 27),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag_name = 'bq_events_to_amplitude'

# This connection needs access to both BQ and GCS
gcp_conn_id = 'google_cloud_derived_datasets'

amplitude_s3_conn = 'amplitude_s3_conn'
amplitude_s3_bucket = 'com-amplitude-vacuum-mozilla-vacuum-wup'

with models.DAG(
        dag_name,
        schedule_interval='0 1 * * *',
        default_args=default_args) as dag:

    # TODO update view
    # For now, we assume the view is already updated
    # See https://github.com/mozilla/bigquery-etl/issues/218

    # Overwrite the table with yesterday's data
    table_name = 'fenix_events_amplitude_v1'
    table_creation = bigquery_etl_query(
        gke_cluster_name='bq-load-gke-2',
        destination_table=table_name,
        date_partition_parameter=None,
        parameters=('submission_date' + ":DATE:{{ds}}",)
    )

    project_id = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id

    gcs_bucket = "moz-fx-data-amplitude-export"
    directory = 'fenix/{{ ds_nodash }}/'
    extension = '.tsv.gz'

    # Export from bq to gcs
    # Docs: https://github.com/apache/airflow/blob/master/airflow/contrib/operators/bigquery_to_gcs.py#L28
    table_extract = BigQueryToCloudStorageOperator(
        task_id='bq_to_gcs', 
        source_project_dataset_table='{project}.telemetry.{table}'.format(project=project_id, table=table_name),
        destination_cloud_storage_uris=['gs://{bucket}/{dir}*{ext}'.format(bucket=gcs_bucket, dir=directory, ext=extension)],
        bigquery_conn_id=gcp_conn_id,
        compression='GZIP',
        export_format='CSV',
        field_delimiter='\t',
        print_header=True
    )

    # Push the data to S3
    # Docs: https://github.com/apache/airflow/blob/master/airflow/contrib/operators/gcs_to_s3.py#L29
    s3_push = GoogleCloudStorageToS3Operator(
        task_id='gcs_to_s3',
        bucket=gcs_bucket,
        prefix=directory,
        delimiter=extension,
        google_cloud_storage_conn_id=gcp_conn_id,
        dest_aws_conn_id=amplitude_s3_conn,
        dest_s3_key='s3://{bucket}/{dir}'.format(bucket=amplitude_s3_bucket, dir=directory),
        replace=True
    )

    table_creation >> table_extract >> s3_push
