from airflow import DAG
from datetime import datetime, timedelta

from operators.bq_sensor import BigQuerySQLSensorOperator

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
         schedule_interval='@daily') as dag:


    wait_for_copy_deduplicate = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        dag=dag)

    table = "incline_executive_v1"
    project = "moz-fx-data-shared-prod"
    dataset = "org_mozilla_firefox_derived"

    exec_dash = bigquery_etl_query(
        task_id="incline_exec_dash",
        destination_table=table,
        project_id=project,
        dataset_id=dataset,
        owner="frank@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
        dag=dag)

    sql = (
        'SELECT * '
        'FROM `{project}.{dataset}.{table}'
        'WHERE date = "{{ ds }}"'
    ).format(project=project, dataset=dataset, table=table)

    gcp_conn_id = 'google_cloud_derived_datasets',
    fully_qualified_tmp_table = "{project}.tmp.{table}_{{ds}}".format(project=project, table=table)

    create_table = BigQueryOperator(
        task_id='create_temporary_table',
        sql=sql,
        destination_dataset_table=fully_qualified_tmp_table,
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False
    )


    fully_qualified_table_name = '{}.{}.{}'.format(project, dataset, table)
    gcs_bucket = 'moz-fx-data-prod-analysis'
    incline_prefix = 'incline/executive_dash/{{ ds_nodash }}/data.csv'
    gcs_uri = 'gs://{bucket}/{prefix}'.format(bucket=gcs_bucket, prefix=incline_prefix)

    table_extract = BigQueryToCloudStorageOperator(
        task_id='bq_to_gcs',
        source_project_dataset_table=fully_qualified_table_name,
        destination_cloud_storage_uris=[gcs_uri],
        bigquery_conn_id=gcp_conn_id,
        export_format='JSON',
        compression='GZIP',
        print_header=True
    )

    # Drop the temporary table
    table_drop = BigQueryOperator(
        task_id='drop_temp_table',
        sql='DROP TABLE `{}`'.format(fully_qualified_tmp_table),
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False
    )

    wait_for_data >> table_extract >> copy_table
