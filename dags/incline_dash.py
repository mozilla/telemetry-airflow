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
        project_id=project,
        dataset_id=dataset,
        # We recreate this entire table from scratch every day because we are
        # taking the last seen migration ping over all time for each client.
        destination_table=None,
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

    sql = (
        'SELECT * FROM `{project}.{dataset}.{table}` WHERE date = "{date}"'
    ).format(project=project, dataset=dataset, table=table, date="{{ ds }}")

    gcp_conn_id = 'google_cloud_derived_datasets'
    fully_qualified_tmp_table = (
        "{project}.tmp.{table}_{date}"
        .format(project=project, table=table, date="{{ ds_nodash }}")
    )

    create_table = BigQueryOperator(
        task_id='create_temporary_table',
        sql=sql,
        destination_dataset_table=fully_qualified_tmp_table,
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False
    )

    fully_qualified_table_name = '{}.{}.{}'.format(project, dataset, table)
    gcs_bucket = 'moz-fx-data-prod-analysis'
    incline_prefix = 'incline/executive_dash/{date}/data.ndjson'.format(date="{{ ds }}")
    gcs_uri = 'gs://{bucket}/{prefix}'.format(bucket=gcs_bucket, prefix=incline_prefix)

    table_extract = BigQueryToCloudStorageOperator(
        task_id='extract_as_latest',
        source_project_dataset_table=fully_qualified_table_name,
        destination_cloud_storage_uris=[gcs_uri],
        bigquery_conn_id=gcp_conn_id,
        export_format='JSON',
        compression='GZIP'
    )

    # Drop the temporary table
    table_drop = BigQueryOperator(
        task_id='drop_temp_table',
        sql='DROP TABLE `{}`'.format(fully_qualified_tmp_table),
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False
    )

    wait_for_copy_deduplicate >> \
        migrated_clients >> \
        exec_dash >> \
        create_table >> \
        table_extract >> \
        table_drop
