from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)


def prio_processor_staging_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    gcp_conn_id,
    service_account,
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
            metadata={"PIP_PACKAGES": "click"},
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            **shared_config
        )

        run_dataproc_spark = DataProcPySparkOperator(
            task_id="run_dataproc_spark",
            main="gs://moz-fx-data-prio-bootstrap/runner.py",
            pyfiles=["gs://moz-fx-data-prio-bootstrap/prio_processor.egg"],
            arguments=[
                "--date",
                "{{ ds }}",
                "--input",
                "gs://moz-fx-data-stage-data/telemetry-decoded_gcs-sink-doctype_prio/output",
                "--output",
                "gs://moz-fx-data-prio-data/staging/",
            ],
            **shared_config
        )

        delete_dataproc_cluster = DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster", trigger_rule="all_done", **shared_config
        )
        create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
        return dag
