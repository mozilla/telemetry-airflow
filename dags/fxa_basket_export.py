from datetime import datetime, timedelta

from airflow import models

# Backported 1.10.2 bqoperator
from operators.backport.bigquery_operator_1_10_2 import BigQueryOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from operators.bq_sensor import BigQuerySQLSensorOperator


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime(2020, 7, 21),
    "email": [
        "telemetry-alerts@mozilla.com",
        "jklukas@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

GCS_BUCKET = "fxa-prod-basket"
GCS_PREFIX = "last-active-timestamp"

with models.DAG("adjust_import", default_args=default_args, schedule_interval="15 1 * * *"):
    # Create the table with yesterday's data
    gcp_conn_id = 'google_cloud_derived_datasets'
    project_id = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id
    dataset = 'temp'
    temp_table_name = 'fxa_basket_export_{{ ds_nodash }}'
    fully_qualified_table_name = '{}.{}.{}'.format(project_id, dataset, temp_table_name)

    sql = """
    SELECT
      jsonPayload.fields.uid,
      UNIX_SECONDS(MIN(timestamp)) AS timestamp
    FROM
      `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_{{ ds_nodash }}`
    WHERE
      jsonPayload.type = 'activityEvent'
      AND jsonPayload.fields.event = 'account.signed'
    GROUP BY
      1
    ORDER BY
      2 ASC;
    """

    create_table = BigQueryOperator(
        task_id='create_temporary_table',
        sql=sql,
        destination_dataset_table=fully_qualified_table_name,
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
    )

    directory = '/'.join((GCS_PREFIX, '{{ ds_nodash }}'))
    extension = '.csv'

    # Export from bq to gcs
    # Docs: https://github.com/apache/airflow/blob/master/airflow/contrib/operators/bigquery_to_gcs.py#L28 # noqa: E501
    gcs_uri = 'gs://{}/{}/*{}'.format(GCS_BUCKET, directory, extension)
    table_extract = BigQueryToCloudStorageOperator(
        task_id='bq_to_gcs',
        source_project_dataset_table=fully_qualified_table_name,
        destination_cloud_storage_uris=[gcs_uri],
        bigquery_conn_id=gcp_conn_id,
        compression='GZIP',
        export_format='CSV',
        field_delimiter=',',
        print_header=False,
    )

    # Drop the temporary table
    table_drop = BigQueryOperator(
        task_id='drop_temp_table',
        sql='DROP TABLE `{}`'.format(fully_qualified_table_name),
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False
    )

    create_table >> table_extract >> table_drop
