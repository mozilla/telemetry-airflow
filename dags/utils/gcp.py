from airflow import models
from airflow.utils import trigger_rule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcSparkOperator # noqa
from operators.gcp_container_operator import GKEPodOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator # noqa
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa


def load_to_bigquery(parent_dag_name=None,
                     default_args=None,
                     dataset_s3_bucket=None,
                     aws_conn_id=None,
                     dataset=None,
                     dataset_version=None,
                     gke_cluster_name=None,
                     date_submission_col='submission_date_s3',
                     ds_type='ds_nodash',
                     dag_name='load_to_bigquery',
                     gke_location='us-central1-a',
                     gke_namespace='default',
                     docker_image='docker.io/mozilla/parquet2bigquery:20190415', # noqa
                     reprocess=False,
                     p2b_concurrency='10',
                     p2b_resume=False,
                     p2b_table_alias=None,
                     objects_prefix=None,
                     spark_gs_dataset_location=None,
                     bigquery_dataset='telemetry',
                     dataset_gcs_bucket='moz-fx-data-derived-datasets-parquet',
                     gcp_conn_id='google_cloud_derived_datasets'):

    """ Load Parquet data into BigQuery. Used with SubDagOperator.

    We use S3ToGoogleCloudStorageTransferOperator to create a GCS Transfer
    Service job to transfer the AWS S3 parquet data into a GCS Bucket.
    Once that is completed we launch a Kubernates pod on a existing GKE
    cluster using the GKEPodOperator.

    :param str parent_dag_name:            parent dag name
    :param dict default_args:              dag configuration
    :param str dataset_s3_bucket:          source S3 Bucket
    :param str dataset_gcs_bucket:         destination GCS Bucket
    :param str aws_conn_id:                airflow connection id for S3 access
    :param str gcp_conn_id:                airflow connection id for GCP access
    :param str dataset:                    dataset name
    :param str dataset_version:            dataset version
    :param str date_submission_col:        dataset date submission column
    :param str ds_type:                    dataset format (ds or ds_nodash)
    :param str gke_location:               GKE cluster zone
    :param str gke_namespace:              GKE cluster namespace
    :param str docker_image:               docker image to use for GKE pod operations # noqa
    :param str bigquery_dataset:           bigquery load destination dataset
    :param str p2b_concurrency:            number of processes for parquet2bigquery load
    :param str p2b_table_alias:            override p2b table name with alias
    :param str p2b_resume                  allow resume support. defaults to False
    :param bool reprocess:                 enable dataset reprocessing defaults to False
    :param str objects_prefix:             custom objects_prefix to override defaults
    :param str spark_gs_dataset_location:  custom spark dataset load location to override defaults

    :return airflow.models.DAG
    """

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    _dag_name = '{}.{}'.format(parent_dag_name, dag_name)

    if objects_prefix:
        _objects_prefix = objects_prefix
    else:
        _objects_prefix = '{}/{}/{}={{{{{}}}}}'.format(dataset,
                                                       dataset_version,
                                                       date_submission_col,
                                                       ds_type)
    gcs_buckets = {
        'transfer': dataset_gcs_bucket,
        'load': dataset_gcs_bucket,
    }

    gcstj_object_conditions = {
        'includePrefixes':  _objects_prefix
    }

    gcstj_transfer_options = {
        'deleteObjectsUniqueInSink': True
    }

    gke_args = [
        '-d', bigquery_dataset,
        '-c', p2b_concurrency,
        '-b', gcs_buckets['load'],
        ]

    if not p2b_resume:
        gke_args += ['-R']

    if p2b_table_alias:
        gke_args += ['-a', p2b_table_alias]

    if reprocess:
        reprocess_objects_prefix = _objects_prefix.replace('_nodash', '')
        gcs_buckets['transfer'] += '-tmp'
        gke_args += ['-p', reprocess_objects_prefix]

    else:
        gke_args += ['-p', _objects_prefix]

    bq_table_name = p2b_table_alias or '{}_{}'.format(dataset, dataset_version)

    with models.DAG(_dag_name, default_args=default_args) as dag:
        s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
            task_id='s3_to_gcs',
            s3_bucket=dataset_s3_bucket,
            gcs_bucket=gcs_buckets['transfer'],
            description=_objects_prefix,
            aws_conn_id=aws_conn_id,
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            object_conditions=gcstj_object_conditions,
            transfer_options=gcstj_transfer_options
            )

        reprocess = SubDagOperator(
            subdag=reprocess_parquet(
                _dag_name,
                default_args,
                reprocess,
                gcp_conn_id,
                gcs_buckets,
                _objects_prefix,
                date_submission_col,
                dataset,
                dataset_version,
                gs_dataset_location=spark_gs_dataset_location),
            task_id='reprocess_parquet')

        remove_bq_table = BigQueryTableDeleteOperator(
            bigquery_conn_id=gcp_conn_id,
            deletion_dataset_table='{}.{}${{{{ds_nodash}}}}'.format(bigquery_dataset, bq_table_name), # noqa
            ignore_if_missiing=True
        )

        bulk_load = GKEPodOperator(
            task_id='bigquery_load',
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            location=gke_location,
            cluster_name=gke_cluster_name,
            name=_dag_name.replace('_', '-'),
            namespace=gke_namespace,
            image=docker_image,
            arguments=gke_args,
            )

        s3_to_gcs >> reprocess >> remove_bq_table >> bulk_load

        return dag


def reprocess_parquet(parent_dag_name,
                      default_args,
                      reprocess,
                      gcp_conn_id,
                      gcs_buckets,
                      objects_prefix,
                      date_submission_col,
                      dataset,
                      dataset_version,
                      gs_dataset_location=None,
                      dataproc_zone='us-central1-a',
                      dag_name='reprocess_parquet',
                      num_preemptible_workers=10):

    """ Reprocess Parquet datasets to conform with BigQuery Parquet loader.

    This function should be invoked as part of `load_to_bigquery`.

    https://github.com/mozilla-services/spark-parquet-to-bigquery/blob/master/src/main/scala/com/mozilla/dataops/spark/TransformParquet.scala ## noqa

    :param str parent_dag_name:            parent dag name
    :param dict default_args:              dag configuration
    :param str gcp_conn_id:                airflow connection id for GCP access
    :param dict gcp_buckets:               source and dest gcp buckets for reprocess
    :param str dataset:                    dataset name
    :param str dataset_version:            dataset version
    :param str object_prefix               objects location
    :param str date_submission_col:        dataset date submission column
    :param str dataproc_zone:              GCP zone to launch dataproc clusters
    :param str dag_name:                   name of dag
    :param int num_preemptible_workers:    number of dataproc cluster workers to provision
    :param bool reprocess:                 enable dataset reprocessing. defaults to False
    :param str gs_dataset_location:        override source location, defaults to None

    :return airflow.models.DAG
    """

    JAR = [
        'gs://moz-fx-data-derived-datasets-parquet-tmp/jars/spark-parquet-to-bigquery-assembly-1.0.jar' # noqa
    ]

    if gs_dataset_location:
        _gs_dataset_location = gs_dataset_location
    else:
        _gs_dataset_location = 'gs://{}/{}'.format(gcs_buckets['transfer'],
                                                   objects_prefix)

    cluster_name = '{}-{}'.format(dataset.replace('_', '-'),
                                  dataset_version) + '-{{ ds_nodash }}'

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    spark_args = [
        '--files', _gs_dataset_location,
        '--submission-date-col', date_submission_col,
        '--gcp-project-id', connection.project_id,
        '--gcs-bucket', 'gs://{}'.format(gcs_buckets['load']),
    ]

    _dag_name = '%s.%s' % (parent_dag_name, dag_name)

    with models.DAG(
            _dag_name,
            default_args=default_args) as dag:

        if reprocess:
            create_dataproc_cluster = DataprocClusterCreateOperator(
                task_id='create_dataproc_cluster',
                cluster_name=cluster_name,
                gcp_conn_id=gcp_conn_id,
                project_id=connection.project_id,
                num_workers=2,
                image_version='1.3',
                storage_bucket=gcs_buckets['transfer'],
                zone=dataproc_zone,
                master_machine_type='n1-standard-8',
                worker_machine_type='n1-standard-8',
                num_preemptible_workers=num_preemptible_workers,
                metadata={
                    'gcs-connector-version': '1.9.6',
                    'bigquery-connector-version': '0.13.6'
                    })

            run_dataproc_spark = DataProcSparkOperator(
                task_id='run_dataproc_spark',
                cluster_name=cluster_name,
                dataproc_spark_jars=JAR,
                main_class='com.mozilla.dataops.spark.TransformParquet',
                arguments=spark_args,
                gcp_conn_id=gcp_conn_id)

            delete_dataproc_cluster = DataprocClusterDeleteOperator(
                task_id='delete_dataproc_cluster',
                cluster_name=cluster_name,
                gcp_conn_id=gcp_conn_id,
                project_id=connection.project_id,
                trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

            create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster # noqa

        else:
            DummyOperator(task_id='no_reprocess')

        return dag
