from airflow import models
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Backported 1.10.2 bqoperator
from operators.backport.bigquery_operator_1_10_2 import BigQueryOperator

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from operators.bq_sensor import BigQuerySQLSensorOperator
from os import environ


def export_to_amplitude(
        parent_dag_name,
        dag_name,
        default_args,
        project,
        dataset,
        table_or_view,
        s3_prefix,
        gcs_bucket='moz-fx-data-derived-datasets-amplitude-export',
        gcp_conn_id='google_cloud_derived_datasets',
        amplitude_s3_conn='amplitude_s3_conn',
        amplitude_s3_bucket='com-amplitude-vacuum-mozilla-vacuum-wup'):

    """Export a bigquery table or view to Amplitude.

    This uses the BigQueryToCloudStorage operator to export the
    partition to GCS, then pushes that data to S3. It operates
    on a temporary table that is dropped after the job is finished.

    :param str parent_dag_name: Parent dag name
    :param str dag_name: This dag's name (appended to parent_dag_name)
    :param str default_args: DAG configuration
    :param str dataset: BigQuery project containing the table to be exported
    :param str dataset: BigQuery dataset
    :param str table_or_view: Table or view name
    :param str gcs_bucket: The bucket the data will be exported to
    :param str gcp_conn_id: GCP connection ID
    :param str amplitude_s3_conn: S3 connection ID
    :param str amplitude_s3_bucket: The bucket to export data to
    :param str s3_prefix: The prefix for the s3 objects
    """

    environment = environ['DEPLOY_ENVIRONMENT']
    _dag_name = '{}.{}'.format(parent_dag_name, dag_name)

    with models.DAG(_dag_name, default_args=default_args) as dag:
        # For now, we assume the view is already updated
        # See https://github.com/mozilla/bigquery-etl/issues/218

        exec_date = '{{ ds }}'

        # Check that we have data for this date
        check_sql = (
            'SELECT COUNT(*) '
            'FROM `{}.{}.{}` '
            'WHERE DATE(submission_timestamp) = "{}"'
        ).format(project, dataset, table_or_view, exec_date)

        wait_for_data = BigQuerySQLSensorOperator(
                task_id='wait_for_data',
                sql=check_sql,
                bigquery_conn_id=gcp_conn_id,
                use_legacy_sql=False
        )

        # Create the table with yesterday's data
        project_id = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id
        temp_table_name = table_or_view + '_{{ ds_nodash }}'
        fully_qualified_table_name = '{}.{}.{}'.format(project_id, dataset, temp_table_name)

        sql = (
            'SELECT * EXCEPT (submission_timestamp) '
            'FROM `{}.{}.{}` '
            'WHERE DATE(submission_timestamp) = "{}"'
        ).format(project, dataset, table_or_view, exec_date)

        create_table = BigQueryOperator(
            task_id='create_temporary_table',
            sql=sql,
            destination_dataset_table=fully_qualified_table_name,
            bigquery_conn_id=gcp_conn_id,
            use_legacy_sql=False
        )

        directory = '/'.join((environment, s3_prefix, '{{ ds_nodash }}'))
        extension = '.tsv.gz'

        # Export from bq to gcs
        # Docs: https://github.com/apache/airflow/blob/master/airflow/contrib/operators/bigquery_to_gcs.py#L28 # noqa: E501
        gcs_uri = 'gs://{}/{}/*{}'.format(gcs_bucket, directory, extension)
        table_extract = BigQueryToCloudStorageOperator(
            task_id='bq_to_gcs',
            source_project_dataset_table=fully_qualified_table_name,
            destination_cloud_storage_uris=[gcs_uri],
            bigquery_conn_id=gcp_conn_id,
            compression='GZIP',
            export_format='CSV',
            field_delimiter='\t',
            print_header=True
        )

        # Push the data to S3
        # Docs: https://github.com/apache/airflow/blob/master/airflow/contrib/operators/gcs_to_s3.py#L29 # noqa: E501
        s3_push = GoogleCloudStorageToS3Operator(
            task_id='gcs_to_s3',
            bucket=gcs_bucket,
            prefix=directory,
            delimiter=extension,
            google_cloud_storage_conn_id=gcp_conn_id,
            dest_aws_conn_id=amplitude_s3_conn,
            dest_s3_key='s3://{}/'.format(amplitude_s3_bucket),
            replace=True
        )

        # Drop the temporary table
        table_drop = BigQueryOperator(
            task_id='drop_temp_table',
            sql='DROP TABLE `{}`'.format(fully_qualified_table_name),
            bigquery_conn_id=gcp_conn_id,
            use_legacy_sql=False
        )

        # Delete the GCS data
        data_delete = GoogleCloudStorageDeleteOperator(
            task_id='delete_gcs_data',
            bucket_name=gcs_bucket,
            prefix=directory,
            google_cloud_storage_conn_id=gcp_conn_id
        )

        wait_for_data >> create_table >> table_extract >> s3_push
        s3_push >> table_drop
        s3_push >> data_delete

        return dag
