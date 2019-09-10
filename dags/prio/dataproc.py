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
    dataproc_zone="us-central1-a",
    num_preemptible_workers=10,
):
    """Run the PySpark job for unnesting and range-partitioning Prio pings from
    the ingestion service.

    :param str parent_dag_name:         Name of the parent DAG.
    :param str child_dag_name:          Name of the child DAG.
    :param Dict[str, Any] default_args: Default arguments for the child DAG.
    :param str gcp_conn_id:             Name of the connection string.
    :param str service_account:         The address of the service account.
    :param str dataproc_zone:           The region of the DataProc cluster.
    :param str main:
    :param List[str] pyfiles:
    :param List[str] arguments:
    :param int num_preemptible_workers: The number of preemptible workers.
    :return: DAG
    """

    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    shared_config = {
        "cluster_name": "prio-staging",
        "gcp_conn_id": gcp_conn_id,
        "project_id": connection.project_id,
    }

    with DAG(
        "{}.{}".format(parent_dag_name, child_dag_name), default_args=default_args
    ) as dag:
        create_dataproc_cluster = DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            num_workers=2,
            image_version="1.4",
            zone=dataproc_zone,
            service_account=service_account,
            master_machine_type="n1-standard-8",
            worker_machine_type="n1-standard-8",
            num_preemptible_workers=num_preemptible_workers,
            metadata={"PIP_PACKAGES": "click jsonschema gcsfs==0.2.3"},
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            **shared_config
        )

        run_dataproc_spark = DataProcPySparkOperator(
            task_id="run_dataproc_spark",
            main=main,
            dataproc_pyspark_jars=["gs://spark-lib/bigquery/spark-bigquery-latest.jar"],
            pyfiles=pyfiles,
            arguments=arguments,
            **shared_config
        )

        delete_dataproc_cluster = DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster", trigger_rule="all_done", **shared_config
        )
        create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
        return dag
