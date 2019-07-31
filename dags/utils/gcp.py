from airflow import models
from airflow.utils import trigger_rule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcSparkOperator, DataProcPySparkOperator # noqa
from operators.gcp_container_operator import GKEPodOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator # noqa
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa

import re


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
                     docker_image='docker.io/mozilla/parquet2bigquery:20190722', # noqa
                     reprocess=False,
                     p2b_concurrency='10',
                     p2b_resume=False,
                     p2b_table_alias=None,
                     objects_prefix=None,
                     spark_gs_dataset_location=None,
                     bigquery_dataset='telemetry',
                     dataset_gcs_bucket='moz-fx-data-derived-datasets-parquet',
                     gcp_conn_id='google_cloud_derived_datasets',
                     cluster_by=(),
                     drop=(),
                     rename={},
                     replace=()):

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
    :param List[str] cluster_by:           top level fields to cluster by when creating destination table
    :param List[str] drop:                 top level fields to exclude from destination table
    :param Dict[str, str] rename:          top level fields to rename in destination table
    :param List[str] replace:              top level field replacement expressions

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

    if cluster_by:
        gke_args += ['--cluster-by'] + cluster_by

    if drop:
        gke_args += ['--drop'] + drop

    if rename:
        gke_args += ['--rename'] + [k + "=" + v for k, v in rename.items()]

    if replace:
        gke_args += ['--replace'] + replace

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
            task_id='remove_bq_table',
            bigquery_conn_id=gcp_conn_id,
            deletion_dataset_table='{}.{}${{{{ds_nodash}}}}'.format(bigquery_dataset, bq_table_name), # noqa
            ignore_if_missing=True,
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


def export_to_parquet(
    table,
    arguments=[],
    dag_name="export_to_parquet",
    parent_dag_name=None,
    default_args=None,
    aws_conn_id="aws_dev_iam_s3",
    gcp_conn_id="google_cloud_derived_datasets",
    dataproc_zone="us-central1-a",
    dataproc_storage_bucket="moz-fx-data-derived-datasets-parquet",
    num_preemptible_workers=0,
):

    """ Export a BigQuery table to Parquet.

    https://github.com/mozilla/bigquery-etl/blob/master/script/pyspark/export_to_parquet.py

    :param str table:                             [Required] BigQuery table name
    :param List[str] arguments:                   Additional pyspark arguments
    :param str dag_name:                          Name of DAG
    :param Optional[str] parent_dag_name:         Parent DAG name
    :param Optional[Dict[str, Any]] default_args: DAG configuration
    :param str gcp_conn_id:                       Airflow connection id for GCP access
    :param str dataproc_storage_bucket:           Dataproc staging GCS bucket
    :param str dataproc_zone:                     GCP zone to launch dataproc clusters
    :param int num_preemptible_workers:           Number of Dataproc preemptible workers

    :return: airflow.models.DAG
    """

    # limit cluster name to 42 characters then suffix with -YYYYMMDD
    cluster_name = table.replace("_", "-")
    if len(cluster_name) > 42:
        if cluster_name.rsplit("-v", 1)[-1].isdigit():
            prefix, version = cluster_name.rsplit("-v", 1)
            cluster_name = prefix[:40 - len(version)] + "-v" + version
        else:
            cluster_name = cluster_name[:42]
    cluster_name += "-{{ ds_nodash }}"

    dag_prefix = parent_dag_name + "." if parent_dag_name else ""
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)
    properties = {
        "core:fs.s3a." + key: value
        for key, value in zip(
            ("access.key", "secret.key", "session.token"),
            AwsHook(aws_conn_id).get_credentials(),
        )
        if value is not None
    }

    with models.DAG(dag_id=dag_prefix + dag_name, default_args=default_args) as dag:

        create_dataproc_cluster = DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            cluster_name=cluster_name,
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            properties=properties,
            num_workers=2,
            image_version="1.3",
            storage_bucket=dataproc_storage_bucket,
            zone=dataproc_zone,
            master_machine_type="n1-standard-8",
            worker_machine_type="n1-standard-8",
            num_preemptible_workers=num_preemptible_workers,
        )

        run_dataproc_pyspark = DataProcPySparkOperator(
            task_id="run_dataproc_pyspark",
            cluster_name=cluster_name,
            dataproc_pyspark_jars=[
                "gs://mozilla-bigquery-etl/jars/spark-bigquery-0.5.1-beta-SNAPSHOT.jar"
            ],
            main="https://raw.githubusercontent.com/mozilla/bigquery-etl/master"
            "/script/pyspark/export_to_parquet.py",
            arguments=[table] + arguments,
            gcp_conn_id=gcp_conn_id,
        )

        delete_dataproc_cluster = DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster",
            cluster_name=cluster_name,
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        )

        create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster

        return dag


def bigquery_etl_query(
    destination_table,
    dataset_id,
    parameters=(),
    arguments=(),
    project_id=None,
    sql_file_path=None,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="mozilla/bigquery-etl:latest",
    image_pull_policy="Always",
    date_partition_parameter="submission_date",
    **kwargs
):
    """ Generate.

    :param str destination_table:                  [Required] BigQuery destination table
    :param str dataset_id:                         [Required] BigQuery default dataset id
    :param Tuple[str] parameters:                  Parameters passed to bq query
    :param Tuple[str] arguments:                   Additional bq query arguments
    :param Optional[str] project_id:               BigQuery default project id
    :param Optional[str] sql_file_path:            Optional override for path to the
                                                   SQL query file to run
    :param str gcp_conn_id:                        Airflow connection id for GCP access
    :param str gke_location:                       GKE cluster location
    :param str gke_cluster_name:                   GKE cluster name
    :param str gke_namespace:                      GKE cluster namespace
    :param str docker_image:                       docker image to use
    :param str image_pull_policy:                  Kubernetes policy for when to pull
                                                   docker_image
    :param Optional[str] date_partition_parameter: Parameter for indicating destination
                                                   partition to generate, if None
                                                   destination should be whole table
                                                   rather than partition
    :param Dict[str, Any] kwargs:                  Additional keyword arguments for
                                                   GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["task_id"] = kwargs.get("task_id", destination_table)
    kwargs["name"] = kwargs.get("name", kwargs["task_id"].replace("_", "-"))
    sql_file_path = sql_file_path or "sql/{}/{}.sql".format(dataset_id, destination_table)
    if date_partition_parameter is not None:
        destination_table = destination_table + "${{ds_nodash}}"
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)
    return GKEPodOperator(
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=["query"]
        + ["--destination_table=" + destination_table]
        + ["--dataset_id=" + dataset_id]
        + (["--project_id=" + project_id] if project_id else [])
        + ["--parameter=" + parameter for parameter in parameters]
        + list(arguments)
        + [sql_file_path],
        image_pull_policy=image_pull_policy,
        **kwargs
    )


def bigquery_etl_copy_deduplicate(
    task_id,
    target_project_id,
    only_tables=None,
    except_tables=None,
    parallelism=4,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="mozilla/bigquery-etl:latest",
    image_pull_policy="Always",
    **kwargs
):
    """ Copy a day's data from live ping tables to stable ping tables,
    deduplicating on document_id.

    :param str task_id:              [Required] ID for the task
    :param str target_project_id:    [Required] ID of project where target tables live
    :param Tuple[str] only_tables:   Only process tables matching the given globs of form 'telemetry_live.main_v*'
    :param Tuple[str] except_tables: Process all tables except those matching the given globs
    :param int parallelism:          Maximum number of queries to execute concurrently
    :param str gcp_conn_id:          Airflow connection id for GCP access
    :param str gke_location:         GKE cluster location
    :param str gke_cluster_name:     GKE cluster name
    :param str gke_namespace:        GKE cluster namespace
    :param str docker_image:         docker image to use
    :param str image_pull_policy:    Kubernetes policy for when to pull
                                     docker_image
    :param Dict[str, Any] kwargs:    Additional keyword arguments for
                                     GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))
    table_qualifiers = []
    if only_tables:
        table_qualifiers.append('--only')
        table_qualifiers += only_tables
    if except_tables:
        table_qualifiers.append('--except')
        table_qualifiers += except_tables
    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=["script/copy_deduplicate"]
        + ["--project_id=" + target_project_id]
        + ["--date={{ds}}"]
        + ["--parallelism={}".format(parallelism)]
        + table_qualifiers,
        image_pull_policy=image_pull_policy,
        **kwargs
    )
