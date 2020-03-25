from airflow import DAG
from datetime import datetime, timedelta

from utils.gcp import bigquery_etl_query

from operators.backport.bigquery_operator_1_10_2 import BigQueryOperator

from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


with DAG('incline_dashboard',
         default_args=default_args,
         schedule_interval="0 1 * * *") as dag:

    wait_for_copy_deduplicate = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        dag=dag)

    table = "migrated_clients_v1"
    project = "moz-fx-data-shared-prod"
    dataset = "org_mozilla_firefox_derived"

    migrated_clients = bigquery_etl_query(
        task_id="generate_migrated_clients",
        destination_table=None,
        project_id=project,
        dataset_id=dataset,
        date_partition_parameter=None,
        sql_file_path="sql/org_mozilla_firefox_derived/migrated_clients_v1/init.sql",
        owner="frank@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"]
    )

    table = "incline_executive_v1"

    exec_dash = bigquery_etl_query(
        task_id="generate_incline_exec_dash",
        destination_table=table,
        project_id=project,
        dataset_id=dataset,
        owner="frank@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
        dag=dag)

    gcp_conn_id = 'google_cloud_derived_datasets',
    fully_qualified_table_name = '{}.{}.{}'.format(project, dataset, table)
    gcs_bucket = 'moz-fx-data-prod-analysis'
    incline_prefix = 'incline/executive_dash/{}/data.csv.gz'
    gcs_uri = 'gs://{bucket}/{prefix}'.format(bucket=gcs_bucket, prefix=incline_prefix.format('latest'))

    table_extract = BigQueryToCloudStorageOperator(
        task_id='extract_as_latest',
        source_project_dataset_table=fully_qualified_table_name,
        destination_cloud_storage_uris=[gcs_uri],
        bigquery_conn_id=gcp_conn_id,
        export_format='CSV',
        compression='GZIP'
    )

    data_copy = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='gcs_copy_latest_to_date',
        source_bucket=gcs_bucket,
        source_object=incline_prefix.format('latest'),
        destination_bucket=gcs_bucket,
        destination_object=incline_prefix.format('{{ds}}')
    )

    wait_for_copy_deduplicate >> \
        migrated_clients >> \
        exec_dash >> \
        table_extract >> \
        data_copy
