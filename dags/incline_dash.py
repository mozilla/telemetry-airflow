from airflow import DAG
from datetime import datetime, timedelta

from operators.bq_sensor import BigQuerySQLSensorOperator

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 02, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


with DAG('incline_dashboard',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    gcp_conn_id = 'google_cloud_derived_datasets',
    fully_qualified_table_name = 'moz-fx-data-shared-prod.analysis.f2f__growth_dashboard'

    # Check that we have data for this date
    check_sql = (
        'SELECT COUNT(*) '
        'FROM `{}` '
        'WHERE DATE(submission_timestamp) = "{}"'
    ).format(fully_qualified_table_name, '{{ ds }}')

    wait_for_data = BigQuerySQLSensorOperator(
        task_id='wait_for_data',
        sql=check_sql,
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False
    )

    gcs_bucket = 'moz-fx-data-prod-analysis'
    incline_prefix = 'incline'
    gcs_prefix = incline_prefix + '/latest'
    gcs_uri = 'gs://{bucket}/{prefix}'.format(bucket=gcs_bucket, prefix=gcs_prefix)

    table_extract = BigQueryToCloudStorageOperator(
        task_id='bq_to_gcs',
        source_project_dataset_table=fully_qualified_table_name,
        destination_cloud_storage_uris=[gcs_uri],
        bigquery_conn_id=gcp_conn_id,
        export_format='CSV',
        print_header=True
    )

    copy_table = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='copy_table',
        source_bucket=gcs_bucket,
        source_object=gcs_prefix + '*',
        destination_bucket=gcs_bucket,
        destination_object=incline_prefix + '/{{ ds_nodash }}',
        gcp_conn_id=gcp_conn_id
    )
