from airflow import models
from airflow.utils import trigger_rule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcSparkOperator, DataProcPySparkOperator # noqa
from operators.gcp_container_operator import GKEPodOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator  # noqa:E501
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator  # noqa:E501
from operators.dataproc_hadoop_with_aws import DataProcHadoopOperatorWithAws
from operators.gcs import GoogleCloudStorageDeleteOperator

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
                     docker_image='docker.io/mozilla/parquet2bigquery:20191017', # noqa
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
        'includePrefixes': _objects_prefix
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

    bq_table_name = p2b_table_alias or normalize_table_id('_'.join([dataset,
                                                                   dataset_version]))

    with models.DAG(_dag_name, default_args=default_args) as dag:
        if dataset_s3_bucket is not None:
            s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
                task_id='s3_to_gcs',
                s3_bucket=dataset_s3_bucket,
                gcs_bucket=gcs_buckets['transfer'],
                description=_objects_prefix,
                aws_conn_id=aws_conn_id,
                gcp_conn_id=gcp_conn_id,
                project_id=connection.project_id,
                object_conditions=gcstj_object_conditions,
                transfer_options=gcstj_transfer_options,
            )
        else:
            s3_to_gcs = DummyOperator(task_id='no_s3_to_gcs')

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
    destination_table=None,
    static_partitions=None,
    arguments=[],
    use_storage_api=False,
    dag_name="export_to_parquet",
    parent_dag_name=None,
    default_args=None,
    aws_conn_id="aws_dev_iam_s3",
    gcp_conn_id="google_cloud_derived_datasets",
    dataproc_zone="us-central1-a",
    dataproc_storage_bucket="moz-fx-data-derived-datasets-parquet",
    num_workers=2,
    num_preemptible_workers=0,
    gcs_output_bucket="moz-fx-data-derived-datasets-parquet",
    s3_output_bucket="telemetry-parquet",
):

    """ Export a BigQuery table to Parquet.

    https://github.com/mozilla/bigquery-etl/blob/master/script/pyspark/export_to_parquet.py

    :param str table:                             [Required] BigQuery table name
    :param Optional[str] destination_table:       Output table name, defaults to table,
                                                  will have r'_v[0-9]+$' replaced with
                                                  r'/v[0-9]+'
    :param List[str] arguments:                   Additional pyspark arguments
    :param bool use_storage_api:                  Whether to read from the BigQuery
                                                  Storage API or an AVRO export
    :param str dag_name:                          Name of DAG
    :param Optional[str] parent_dag_name:         Parent DAG name
    :param Optional[Dict[str, Any]] default_args: DAG configuration
    :param str gcp_conn_id:                       Airflow connection id for GCP access
    :param str dataproc_storage_bucket:           Dataproc staging GCS bucket
    :param str dataproc_zone:                     GCP zone to launch dataproc clusters
    :param int num_preemptible_workers:           Number of Dataproc preemptible workers

    :return: airflow.models.DAG
    """

    # remove the dataset prefix and partition suffix from table
    unqualified_table = table.rsplit(".", 1).pop().split("$", 1)[0]
    # limit cluster name to 35 characters plus suffix of -export-YYYYMMDD (51 total)
    cluster_name = unqualified_table.replace("_", "-")
    if len(cluster_name) > 35:
        # preserve version when truncating cluster name to 42 characters
        prefix, version = re.match(r"(.*?)(-v[0-9]+)?$", cluster_name).groups("")
        cluster_name = prefix[:35 - len(version)] + version
    cluster_name += "-export-{{ ds_nodash }}"

    dag_prefix = parent_dag_name + "." if parent_dag_name else ""
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    if destination_table is None:
        destination_table = unqualified_table
    # separate version using "/" instead of "_"
    export_prefix = re.sub(r"_(v[0-9]+)$", r"/\1", destination_table) + "/"
    if static_partitions:
        export_prefix += "/".join(static_partitions) + "/"
    avro_prefix = "avro/" + export_prefix
    avro_path = "gs://" + gcs_output_bucket + "/" + avro_prefix + "*.avro"

    with models.DAG(dag_id=dag_prefix + dag_name, default_args=default_args) as dag:

        create_dataproc_cluster = DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            cluster_name=cluster_name,
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            num_workers=num_workers,
            image_version="1.4",
            storage_bucket=dataproc_storage_bucket,
            zone=dataproc_zone,
            master_machine_type="n1-standard-8",
            worker_machine_type="n1-standard-8",
            num_preemptible_workers=num_preemptible_workers,
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh",
            ],
            metadata={"PIP_PACKAGES": "google-cloud-bigquery==1.20.0"},
        )

        run_dataproc_pyspark = DataProcPySparkOperator(
            task_id="run_dataproc_pyspark",
            cluster_name=cluster_name,
            dataproc_pyspark_jars=[
                "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
            ],
            dataproc_pyspark_properties={
                "spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.4",
            },
            main="https://raw.githubusercontent.com/mozilla/bigquery-etl/master"
            "/script/pyspark/export_to_parquet.py",
            arguments=[table]
            + [
                "--" + key + "=" + value
                for key, value in {
                    "avro-path": (not use_storage_api) and avro_path,
                    "destination": "gs://" + gcs_output_bucket,
                    "destination-table": destination_table,
                }.items()
                if value
            ]
            + ["--static-partitions=" + p for p in static_partitions]
            + arguments,
            gcp_conn_id=gcp_conn_id,
        )

        gcs_to_s3 = DataProcHadoopOperatorWithAws(
            task_id="gcs_to_s3",
            main_jar="file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar",
            arguments=[
                "-update",
                "-delete",
                "gs://{}/{}".format(gcs_output_bucket, export_prefix),
                "s3a://{}/{}".format(s3_output_bucket, export_prefix),
            ],
            cluster_name=cluster_name,
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            aws_conn_id=aws_conn_id,
        )

        delete_dataproc_cluster = DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster",
            cluster_name=cluster_name,
            gcp_conn_id=gcp_conn_id,
            project_id=connection.project_id,
            trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        )

        if not use_storage_api:
            avro_export = BigQueryToCloudStorageOperator(
                task_id="avro_export",
                source_project_dataset_table=table,
                destination_cloud_storage_uris=avro_path,
                compression=None,
                export_format="AVRO",
                bigquery_conn_id=gcp_conn_id,
            )
            avro_delete = GoogleCloudStorageDeleteOperator(
                task_id="avro_delete",
                bucket_name=gcs_output_bucket,
                prefix=avro_prefix,
                gcp_conn_id=gcp_conn_id,
                trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
            )
            avro_export >> run_dataproc_pyspark >> avro_delete

        create_dataproc_cluster >> run_dataproc_pyspark >> gcs_to_s3
        gcs_to_s3 >> delete_dataproc_cluster

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
    multipart=False,
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
    sql_file_path = sql_file_path or "sql/{}/{}/query.sql".format(dataset_id, destination_table)
    if destination_table is not None and date_partition_parameter is not None:
        destination_table = destination_table + "${{ds_nodash}}"
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)
    return GKEPodOperator(
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=["script/run_multipart_query" if multipart else "query"]
        + (["--destination_table=" + destination_table] if destination_table else [])
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
    priority="INTERACTIVE",
    hourly=False,
    slices=None,
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
    :param str priority:             BigQuery query priority to use, must be BATCH or INTERACTIVE
    :param bool hourly:              Alias for --slices=24
    :param int slices:               Number of time-based slices to deduplicate in, rather than for whole days at once
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
        + ["--project-id=" + target_project_id]
        + ["--date={{ds}}"]
        + ["--parallelism={}".format(parallelism)]
        + ["--priority={}".format(priority)]
        + (["--hourly"] if hourly else [])
        + (["--slices={}".format(slices)] if slices is not None else [])
        + table_qualifiers,
        image_pull_policy=image_pull_policy,
        **kwargs
    )


def bigquery_xcom_query(
    destination_table,
    dataset_id,
    xcom_task_id,
    parameters=(),
    arguments=(),
    project_id=None,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="mozilla/bigquery-etl:latest",
    image_pull_policy="Always",
    date_partition_parameter="submission_date",
    **kwargs
):
    """ Generate a GKEPodOperator which runs an xcom result as a bigquery query.

    :param str destination_table:                  [Required] BigQuery destination table
    :param str dataset_id:                         [Required] BigQuery default dataset id
    :param str xcom_task_id:                       [Required] task_id which generated the xcom to pull
    :param Tuple[str] parameters:                  Parameters passed to bq query
    :param Tuple[str] arguments:                   Additional bq query arguments
    :param Optional[str] project_id:               BigQuery default project id
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
    if destination_table is not None and date_partition_parameter is not None:
        destination_table = destination_table + "${{ds_nodash}}"
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)
    query = "{{ " + "task_instance.xcom_pull(task_ids='{}')".format("xcom_task_id") + " }}"
    return GKEPodOperator(
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=["bq"]
        + ["query"]
        + (["--destination_table=" + destination_table] if destination_table else [])
        + ["--dataset_id=" + dataset_id]
        + (["--project_id=" + project_id] if project_id else [])
        + ["--parameter=" + parameter for parameter in parameters]
        + list(arguments)
        + [query],
        image_pull_policy=image_pull_policy,
        **kwargs
    )


def normalize_table_id(table_name):
    """
    Normalize table name for use with BigQuery.
    * Contain up to 1,024 characters
    * Contain letters (upper or lower case), numbers, and underscores
    We intentionally lower case the table_name.
    https://cloud.google.com/bigquery/docs/tables
    """
    if len(table_name) > 1024:
        raise ValueError('table_name cannot contain more than 1024 characters')
    else:
        return re.sub('\W+', '_', table_name).lower()


def gke_command(
    task_id,
    command,
    docker_image,
    aws_conn_id="aws_dev_iam_s3",
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    image_pull_policy="Always",
    **kwargs
):
    """ Run a docker command on GKE

    :param str task_id:            [Required] ID for the task
    :param List[str] command:      [Required] Command to run
    :param str docker_image:       [Required] docker image to use
    :param str aws_conn_id:        Airflow connection id for AWS access
    :param str gcp_conn_id:        Airflow connection id for GCP access
    :param str gke_location:       GKE cluster location
    :param str gke_cluster_name:   GKE cluster name
    :param str gke_namespace:      GKE cluster namespace
    :param str image_pull_policy:  Kubernetes policy for when to pull
                                   docker_image
    :param Dict[str, Any] kwargs:  Additional keyword arguments for
                                   GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))
    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=command,
        env_vars={
            key: value
            for key, value in zip(
                ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"),
                AwsHook(aws_conn_id).get_credentials() if aws_conn_id else (),
            )
            if value is not None
        },
        image_pull_policy=image_pull_policy,
        **kwargs
    )
