from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)


def spark_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    gcp_conn_id,
    service_account,
    main,
    pyfiles,
    arguments,
    bootstrap_bucket,
    dataproc_region="us-west1",
    num_preemptible_workers=10,
):
    """Run the PySpark job for unnesting and range-partitioning Prio pings from
    the ingestion service.

    :param str parent_dag_name:         Name of the parent DAG.
    :param str child_dag_name:          Name of the child DAG.
    :param Dict[str, Any] default_args: Default arguments for the child DAG.
    :param str gcp_conn_id:             Name of the connection string.
    :param str service_account:         The address of the service account.
    :param str dataproc_region:           The region of the DataProc cluster.
    :param str main:
    :param List[str] pyfiles:
    :param List[str] arguments:
    :param int num_preemptible_workers: The number of preemptible workers.
    :return: DAG
    """

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    shared_config = {
        "cluster_name": "prio-staging-{{ds_nodash}}",
        "gcp_conn_id": gcp_conn_id,
        "project_id": connection.project_id,
        # From an error when not specifying the region:
        # - Dataproc images 2.0 and higher do not support the to-be
        #   deprecated global region. Please use any non-global Dataproc
        #   region instead
        #  - Must specify a zone in GCE configuration when using
        #    'regions/global'. To use auto zone placement, specify
        #    regions/<non-global-region> in request path, e.g.
        #    regions/us-central1
        "region": dataproc_region,
    }

    with DAG(f"{parent_dag_name}.{child_dag_name}", default_args=default_args) as dag:
        create_dataproc_cluster = DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            image_version="preview-ubuntu18",
            service_account=service_account,
            master_machine_type="n1-standard-4",
            worker_machine_type="n1-standard-4",
            num_workers=2,
            num_preemptible_workers=num_preemptible_workers,
            init_actions_uris=[f"{bootstrap_bucket}/install-python-requirements.sh"],
            idle_delete_ttl=600,
            dag=dag,
            **shared_config,
        )

        run_dataproc_spark = DataProcPySparkOperator(
            task_id="run_dataproc_spark",
            main=main,
            dataproc_pyspark_jars=[
                "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
            ],
            pyfiles=pyfiles,
            arguments=arguments,
            dag=dag,
            **shared_config,
        )

        delete_dataproc_cluster = DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster",
            trigger_rule="all_done",
            dag=dag,
            **shared_config,
        )
        create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
        return dag
