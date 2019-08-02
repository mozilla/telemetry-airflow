from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)


def create_prio_staging(
    parent_dag_name,
    default_args,
    dataproc_zone="us-central1-a",
    dag_name="prio_staging",
    num_preemptible_workers=10,
):
    shared_config = {
        "cluster_name": "prio-staging",
        "gcp_conn_id": "google_cloud_prio_admin",
        "project_id": "moz-fx-prio-admin",
    }

    with DAG(
        "{}.{}".format(parent_dag_name, dag_name), default_args=default_args
    ) as dag:
        create_dataproc_cluster = DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            num_workers=2,
            image_version="1.4",
            zone=dataproc_zone,
            service_account="prio-admin-runner@moz-fx-prio-admin.iam.gserviceaccount.com",
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
