from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.utils.task_group import TaskGroup

from operators.gcp_container_operator import GKEPodOperator
from utils.dataproc import moz_dataproc_pyspark_runner
from utils.tags import Tag

"""
This uses dataproc to rewrite the data to parquet in gcs, and
load the parquet data into bigquery.

The following WTMO connections are needed in order for this job to run:
conn - google_cloud_airflow_dataproc
conn - google_cloud_airflow_gke
"""

default_args = {
    "owner": "srose@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 10),
    "email": [
        "anicholson@mozilla.com",
        "srose@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "socorro_import",
    default_args=default_args,
    schedule_interval="@daily",
    tags=tags,
) as dag:
    # Unsalted cluster name so subsequent runs fail if the cluster name exists
    cluster_name = "socorro-import-dataproc-cluster"

    # Defined in Airflow's UI -> Admin -> Connections
    gcp_conn_id = "google_cloud_airflow_dataproc"
    project_id = "airflow-dataproc"

    # We use an application-specific gcs bucket because the data needs to be transformed
    # in dataproc before loading

    gcs_data_bucket = "moz-fx-data-prod-socorro-data"

    dataset = "socorro_crash"
    dataset_version = "v2"
    date_submission_col = "crash_date"

    objects_prefix = "{}/{}/{}={}".format(
        dataset, dataset_version, date_submission_col, "{{ ds_nodash }}"
    )

    # Spark job reads gcs json and writes gcs parquet
    crash_report_parquet = SubDagOperator(
        task_id="crash_report_parquet",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            dag_name="crash_report_parquet",
            default_args=default_args,
            cluster_name=cluster_name,
            job_name="Socorro_Crash_Reports_to_Parquet",
            python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/socorro_import_crash_data.py",
            py_args=[
                "--date",
                "{{ ds_nodash }}",
                "--source-gcs-path",
                "gs://moz-fx-socorro-prod-prod-telemetry/v1/crash_report",
                "--dest-gcs-path",
                f"gs://{gcs_data_bucket}/{dataset}",
            ],
            idle_delete_ttl=14400,
            num_workers=8,
            worker_machine_type="n1-standard-8",
            gcp_conn_id=gcp_conn_id,
        ),
    )

    bq_gcp_conn_id = "google_cloud_airflow_gke"

    # Not using load_to_bigquery since our source data is on GCS.
    # We do use the parquet2bigquery container to load gcs parquet into bq though.
    bq_dataset = "telemetry_derived"
    bq_table_name = f"{dataset}_{dataset_version}"

    # This image was manually built from
    # https://github.com/mozilla/parquet2bigquery/commit/6bf1f86076de8939ba2c4d008080d6c159a0a093
    # using python:3.7.4-slim-buster
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/parquet2bigquery:20190722"

    gke_args = [
        "--dataset",
        bq_dataset,
        "--concurrency",
        "10",
        "--bucket",
        gcs_data_bucket,
        "--no-resume",
        "--prefix",
        objects_prefix,
        "--cluster-by",
        "crash_date",
    ]

    # We remove the current date partition for idempotency.
    table_name = "{}:{}.{}${{{{ds_nodash}}}}".format(
        "{{ var.value.gcp_shared_prod_project }}", bq_dataset, bq_table_name
    )

    remove_bq_table_partition = GKEPodOperator(
        task_id="remove_socorro_crash_bq_table_partition",
        gcp_conn_id=bq_gcp_conn_id,
        name="remove_socorro_crash_bq_table_partition",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        arguments=["bq", "rm", "-f", "--table", table_name],
    )

    bq_load = GKEPodOperator(
        task_id="bigquery_load",
        gcp_conn_id=bq_gcp_conn_id,
        name="load-socorro-crash-parquet-to-bq",
        image=docker_image,
        arguments=gke_args,
        env_vars={"GOOGLE_CLOUD_PROJECT": "{{ var.value.gcp_shared_prod_project }}"},
    )

    with TaskGroup("socorro_external") as socorro_external:
        ExternalTaskMarker(
            task_id="crash_symbolication__wait_for_socorro_import",
            external_dag_id="crash_symbolication",
            external_task_id="wait_for_socorro_import",
            execution_date="{{ execution_date.replace(hour=5, minute=0).isoformat() }}",
        )

        bq_load >> socorro_external

    crash_report_parquet >> remove_bq_table_partition >> bq_load
