import re

from airflow import models
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

from operators.gcp_container_operator import GKEPodOperator
from utils.dataproc import get_dataproc_parameters

GCP_PROJECT_ID = "moz-fx-data-airflow-gke-prod"
DATAPROC_PROJECT_ID = "airflow-dataproc"
BIGQUERY_ETL_DOCKER_IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

def export_to_parquet(
    table,
    destination_table=None,
    static_partitions=None,
    arguments=None,
    use_storage_api=False,
    dag_name="export_to_parquet",
    parent_dag_name=None,
    default_args=None,
    gcp_conn_id="google_cloud_airflow_dataproc",
    dataproc_storage_bucket="airflow-dataproc-bq-parquet-exports",
    num_workers=2,
    num_preemptible_workers=0,
    gcs_output_bucket="airflow-dataproc-bq-parquet-exports",
    region="us-west1",
):
    """
    Export a BigQuery table to Parquet.

    https://github.com/mozilla/bigquery-etl/blob/main/script/pyspark/export_to_parquet.py

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
    :param int num_preemptible_workers:           Number of Dataproc preemptible workers
    :param str region:                            Region where the dataproc cluster will
                                                  be located. Zone will be chosen
                                                  automatically

    :return: airflow.models.DAG
    """

    # remove the dataset prefix and partition suffix from table
    table_id = table.rsplit(".", 1)[-1]
    unqualified_table, _, partition_id = table_id.partition("$")
    # limit cluster name to 35 characters plus suffix of -export-YYYYMMDD (51 total)
    cluster_name = unqualified_table.replace("_", "-")
    if len(cluster_name) > 35:
        # preserve version when truncating cluster name to 42 characters
        prefix, version = re.match(r"(.*?)(-v[0-9]+)?$", cluster_name).groups("")
        cluster_name = prefix[: 35 - len(version)] + version
    cluster_name += "-export-{{ ds_nodash }}"

    dag_prefix = parent_dag_name + "." if parent_dag_name else ""
    project_id = DATAPROC_PROJECT_ID

    if destination_table is None:
        destination_table = unqualified_table
    # separate version using "/" instead of "_"
    export_prefix = re.sub(r"_(v[0-9]+)$", r"/\1", destination_table) + "/"
    if static_partitions:
        export_prefix += "/".join(static_partitions) + "/"
    avro_prefix = "avro/" + export_prefix
    if not static_partitions and partition_id:
        avro_prefix += "partition_id=" + partition_id + "/"
    avro_path = "gs://" + gcs_output_bucket + "/" + avro_prefix + "*.avro"

    params = get_dataproc_parameters("google_cloud_airflow_dataproc")

    if arguments is None:
        arguments = []

    with models.DAG(dag_id=dag_prefix + dag_name, default_args=default_args) as dag:
        create_dataproc_cluster = DataprocCreateClusterOperator(
            task_id="create_dataproc_cluster",
            cluster_name=cluster_name,
            project_id=project_id,
            gcp_conn_id=gcp_conn_id,
            region=region,
            cluster_config=ClusterGenerator(
                project_id=project_id,
                service_account=params.client_email,
                num_workers=num_workers,
                storage_bucket=dataproc_storage_bucket,
                init_actions_uris=[
                    # This is probably a google hosted bucket for
                    # https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/python/pip-install.sh
                    "gs://dataproc-initialization-actions/python/pip-install.sh",
                ],
                metadata={"PIP_PACKAGES": "google-cloud-bigquery==1.20.0"},
                image_version="1.4-debian10",
                properties={},
                master_machine_type="n1-standard-8",
                worker_machine_type="n1-standard-8",
                num_preemptible_workers=num_preemptible_workers,
            ).make(),
        )

        run_dataproc_pyspark = DataprocSubmitPySparkJobOperator(
            task_id="run_dataproc_pyspark",
            cluster_name=cluster_name,
            dataproc_jars=["gs://spark-lib/bigquery/spark-2.4-bigquery-latest.jar"],
            dataproc_properties={
                "spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.4",
            },
            main="https://raw.githubusercontent.com/mozilla/bigquery-etl/main"
            "/script/legacy/export_to_parquet.py",
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
            + (["--static-partitions"] if static_partitions else [])
            + (static_partitions if static_partitions else [])
            + arguments,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            region=region,
        )

        delete_dataproc_cluster = DataprocDeleteClusterOperator(
            task_id="delete_dataproc_cluster",
            cluster_name=cluster_name,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            trigger_rule="all_done",
            region=region,
        )

        if not use_storage_api:
            avro_export = BigQueryToGCSOperator(
                task_id="avro_export",
                source_project_dataset_table=table,
                destination_cloud_storage_uris=avro_path,
                compression=None,
                export_format="AVRO",
                gcp_conn_id=gcp_conn_id,
            )
            avro_delete = GCSDeleteObjectsOperator(
                task_id="avro_delete",
                bucket_name=gcs_output_bucket,
                prefix=avro_prefix,
                gcp_conn_id=gcp_conn_id,
                trigger_rule="all_done",
            )
            avro_export >> run_dataproc_pyspark >> avro_delete

        create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster

        return dag


def bigquery_etl_query(
    destination_table,
    dataset_id,
    parameters=(),
    arguments=(),
    project_id=None,
    sql_file_path=None,
    reattach_on_restart=True,
    gcp_conn_id="google_cloud_airflow_gke",
    gke_project_id=GCP_PROJECT_ID,
    gke_location="us-west1",
    gke_cluster_name="workloads-prod-v1",
    gke_namespace="default",
    docker_image=BIGQUERY_ETL_DOCKER_IMAGE,
    date_partition_parameter="submission_date",
    multipart=False,
    is_delete_operator_pod=False,
    table_partition_template="${{ds_nodash}}",
    **kwargs,
):
    """
    Generate.

    :param str destination_table:                  [Required] BigQuery destination table
    :param str dataset_id:                         [Required] BigQuery default dataset id
    :param Tuple[str] parameters:                  Parameters passed to bq query
    :param Tuple[str] arguments:                   Additional bq query arguments
    :param Optional[str] project_id:               BigQuery default project id
    :param Optional[str] sql_file_path:            Optional override for path to the
                                                   SQL query file to run
    :param str gcp_conn_id:                        Airflow connection id for GCP access
    :param str gke_project_id:                     GKE cluster project id
    :param str gke_location:                       GKE cluster location
    :param str gke_cluster_name:                   GKE cluster name
    :param str gke_namespace:                      GKE cluster namespace
    :param str docker_image:                       docker image to use
    :param Optional[str] date_partition_parameter: Parameter for indicating destination
                                                   partition to generate, if None
                                                   destination should be whole table
                                                   rather than partition
    :param Dict[str, Any] kwargs:                  Additional keyword arguments for
                                                   GKEPodOperator
    :param is_delete_operator_pod                  Optional, What to do when the pod reaches its final
                                                   state, or the execution is interrupted.
                                                   If False (default): do nothing, If True: delete the pod
    :param str table_partition_template:           Template for the datestring that's used for
                                                   the partition table id. Defaults to "{{ds_nodash}}".
    :return: GKEPodOperator
    """
    kwargs["task_id"] = kwargs.get("task_id", destination_table)
    kwargs["name"] = kwargs.get("name", kwargs["task_id"].replace("_", "-"))
    if not project_id:
        project_id = "moz-fx-data-shared-prod"
    destination_table_no_partition = (
        destination_table.split("$")[0] if destination_table is not None else None
    )
    sql_file_path = (
        sql_file_path
        or f"sql/{project_id}/{dataset_id}/{destination_table_no_partition}/query.sql"
    )
    if destination_table is not None and date_partition_parameter is not None:
        destination_table = destination_table + table_partition_template
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)
    args = ["script/bqetl", "query", "run-multipart"] if multipart else ["query"]
    return GKEPodOperator(
        reattach_on_restart=reattach_on_restart,
        gcp_conn_id=gcp_conn_id,
        project_id=gke_project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=args
        + (["--destination_table=" + destination_table] if destination_table else [])
        + ["--dataset_id=" + dataset_id]
        + (["--project_id=" + project_id] if project_id else [])
        + ["--parameter=" + parameter for parameter in parameters]
        + list(arguments)
        + [sql_file_path],
        is_delete_operator_pod=is_delete_operator_pod,
        **kwargs,
    )


def bigquery_etl_copy_deduplicate(
    task_id,
    target_project_id,
    billing_projects=(),
    only_tables=None,
    except_tables=None,
    parallelism=4,
    priority="INTERACTIVE",
    hourly=False,
    slices=None,
    gcp_conn_id="google_cloud_airflow_gke",
    gke_project_id=GCP_PROJECT_ID,
    gke_location="us-west1",
    gke_cluster_name="workloads-prod-v1",
    gke_namespace="default",
    docker_image=BIGQUERY_ETL_DOCKER_IMAGE,
    **kwargs,
):
    """
    Copy a day's data from live ping tables to stable ping tables, deduplicating on document_id.

    :param str task_id:              [Required] ID for the task
    :param str target_project_id:    [Required] ID of project where target tables live
    :param Tuple[str] billing_projects: ID of projects where queries will be executed,
                                     defaults to gcp_conn_id project
    :param Tuple[str] only_tables:   Only process tables matching the given globs of form 'telemetry_live.main_v*'
    :param Tuple[str] except_tables: Process all tables except those matching the given globs
    :param int parallelism:          Maximum number of queries to execute concurrently
    :param str priority:             BigQuery query priority to use, must be BATCH or INTERACTIVE
    :param bool hourly:              Alias for --slices=24
    :param int slices:               Number of time-based slices to deduplicate in, rather than for whole days at once
    :param str gcp_conn_id:          Airflow connection id for GCP access
    :param str gke_project_id:       GKE cluster project id
    :param str gke_location:         GKE cluster location
    :param str gke_cluster_name:     GKE cluster name
    :param str gke_namespace:        GKE cluster namespace
    :param str docker_image:         docker image to use
    :param Dict[str, Any] kwargs:    Additional keyword arguments for
                                     GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))
    table_qualifiers = []
    if only_tables:
        # bqetl expects multiple args as --only ... --only ...
        for only_table in only_tables:
            table_qualifiers.append("--only")
            table_qualifiers.append(only_table)
    if except_tables:
        for except_table in except_tables:
            table_qualifiers.append("--except")
            table_qualifiers.append(except_table)
    return GKEPodOperator(
        task_id=task_id,
        reattach_on_restart=True,
        gcp_conn_id=gcp_conn_id,
        project_id=gke_project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=["script/bqetl", "copy_deduplicate"]
        + ["--project-id=" + target_project_id]
        + (["--billing-projects", *list(billing_projects)] if billing_projects else [])
        + ["--date={{ds}}"]
        + [f"--parallelism={parallelism}"]
        + [f"--priority={priority}"]
        + (["--hourly"] if hourly else [])
        + ([f"--slices={slices}"] if slices is not None else [])
        + table_qualifiers,
        **kwargs,
    )


def bigquery_dq_check(
    source_table,
    dataset_id,
    task_id,
    parameters=(),
    arguments=(),
    project_id="moz-fx-data-shared-prod",
    gcp_conn_id="google_cloud_airflow_gke",
    gke_project_id=GCP_PROJECT_ID,
    gke_location="us-west1",
    gke_cluster_name="workloads-prod-v1",
    gke_namespace="default",
    docker_image=BIGQUERY_ETL_DOCKER_IMAGE,
    date_partition_parameter="submission_date",
    is_dq_check_fail=True,
    **kwargs,
):
    """
    Run `bqetl check run` to run data quality checks against BigQuery table.

    :param str destination_table:                  [Required] BigQuery destination table the DQ checks are run on
    :param str dataset_id:                         [Required] BigQuery default dataset id

    :param str task_id:              [             [Required] ID for the task
    :param Optional[str] sql_file_path:            Optional override for path to the
                                                   SQL query file to run
    :param Tuple[str] parameters:                  Parameters passed to bq query
    :param Optional[str] project_id:               BigQuery default project id
    :param str gcp_conn_id:                        Airflow connection id for GCP access
    :param str gke_project_id:                     GKE cluster project id
    :param str gke_location:                       GKE cluster location
    :param str gke_cluster_name:                   GKE cluster name
    :param str gke_namespace:                      GKE cluster namespace
    :param str docker_image:                       docker image to use
    :param Optional[str] date_partition_parameter: Parameter for indicating destination
                                                   partition to generate, if None
                                                   destination should be whole table
                                                   rather than partition
    :param Dict[str, Any] kwargs:                  Additional keyword arguments for
                                                   GKEPodOperator
    :return: GKEPodOperator
    """
    kwargs["task_id"] = kwargs.get("task_id", task_id)
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))
    destination_table_no_partition = (
        source_table.split("$")[0] if source_table is not None else None
    )
    if not date_partition_parameter:
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)

    sql_file_path = f"sql/{project_id}/{dataset_id}/{destination_table_no_partition}"

    marker = ["--marker"]
    if is_dq_check_fail:
        marker += ["fail"]
    else:
        marker += ["warn"]

    args = ["script/bqetl", "check", "run", *marker, sql_file_path]
    return GKEPodOperator(
        gcp_conn_id=gcp_conn_id,
        project_id=gke_project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=args
        + ["--parameter=" + parameter for parameter in parameters]
        + list(arguments),
        **kwargs,
    )

def bigquery_bigeye_check(
    task_id,
    table_id,
    warehouse_id,
    project_id="moz-fx-data-shared-prod",
    gcp_conn_id="google_cloud_airflow_gke",
    gke_project_id=GCP_PROJECT_ID,
    gke_location="us-west1",
    gke_cluster_name="workloads-prod-v1",
    gke_namespace="default",
    docker_image=BIGQUERY_ETL_DOCKER_IMAGE,
    **kwargs,
):
    """
    Run `bqetl monitoring run` to run Bigeye checks against BigQuery table.

    :param str table_id:                           [Required] BigQuery table the Bigeye checks are run against
    :param str warehouse_id:                       [Required] Bigeye warehouse ID where checks located
    :param str task_id:                            [Required] ID for the task
    :param Optional[str] project_id:               BigQuery default project id
    :param str gcp_conn_id:                        Airflow connection id for GCP access
    :param str gke_project_id:                     GKE cluster project id
    :param str gke_location:                       GKE cluster location
    :param str gke_cluster_name:                   GKE cluster name
    :param str gke_namespace:                      GKE cluster namespace
    :param str docker_image:                       docker image to use
    :param Dict[str, Any] kwargs:                  Additional keyword arguments for
                                                   GKEPodOperator
    :return: GKEPodOperator
    """
    kwargs["task_id"] = kwargs.get("task_id", task_id)
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))

    args = ["script/bqetl", "monitoring", "run", table_id, "--warehouse_id", warehouse_id, "--project_id", project_id]

    bigeye_api_key = Secret(
        deploy_type="env",
        deploy_target="BIGEYE_API_KEY",
        secret="airflow-gke-secrets",
        key="bqetl_artifact_deployment__bigeye_api_key"
    )

    return GKEPodOperator(
        gcp_conn_id=gcp_conn_id,
        project_id=gke_project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=docker_image,
        arguments=list(args),
        secrets=[bigeye_api_key],
        **kwargs,
    )


def bigquery_xcom_query(
    destination_table,
    dataset_id,
    xcom_task_id,
    parameters=(),
    arguments=(),
    project_id=None,
    gcp_conn_id="google_cloud_airflow_gke",
    gke_project_id=GCP_PROJECT_ID,
    gke_location="us-west1",
    gke_cluster_name="workloads-prod-v1",
    gke_namespace="default",
    docker_image=BIGQUERY_ETL_DOCKER_IMAGE,
    date_partition_parameter="submission_date",
    table_partition_template="${{ds_nodash}}",
    **kwargs,
):
    """
    Generate a GKEPodOperator which runs an xcom result as a bigquery query.

    :param str destination_table:                  [Required] BigQuery destination table
    :param str dataset_id:                         [Required] BigQuery default dataset id
    :param str xcom_task_id:                       [Required] task_id which generated the xcom to pull
    :param Tuple[str] parameters:                  Parameters passed to bq query
    :param Tuple[str] arguments:                   Additional bq query arguments
    :param Optional[str] project_id:               BigQuery default project id
    :param str gcp_conn_id:                        Airflow connection id for GCP access
    :param str gke_project_id:                     GKE cluster project id
    :param str gke_location:                       GKE cluster location
    :param str gke_cluster_name:                   GKE cluster name
    :param str gke_namespace:                      GKE cluster namespace
    :param str docker_image:                       docker image to use
    :param Optional[str] date_partition_parameter: Parameter for indicating destination
                                                   partition to generate, if None
                                                   destination should be whole table
                                                   rather than partition
    :param str table_partition_template:           Template for the datestring that's used for
                                                   the partition table id. Defaults to "{{ds_nodash}}".
    :param Dict[str, Any] kwargs:                  Additional keyword arguments for
                                                   GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["task_id"] = kwargs.get("task_id", destination_table)
    kwargs["name"] = kwargs.get("name", kwargs["task_id"].replace("_", "-"))
    if destination_table is not None and date_partition_parameter is not None:
        destination_table = destination_table + table_partition_template
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)
    query = "{{ " + f"task_instance.xcom_pull({xcom_task_id!r})" + " }}"

    return GKEPodOperator(
        gcp_conn_id=gcp_conn_id,
        project_id=gke_project_id,
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
        **kwargs,
    )


def normalize_table_id(table_name):
    """
    Normalize table name for use with BigQuery.

    * Contain up to 1,024 characters
    * Contain letters (upper or lower case), numbers, and underscores
    We intentionally lower case the table_name.
    https://cloud.google.com/bigquery/docs/tables.
    """
    if len(table_name) > 1024:
        raise ValueError("table_name cannot contain more than 1024 characters")
    else:
        return re.sub("\\W+", "_", table_name).lower()
