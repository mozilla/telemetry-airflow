"""
Client Lifetime Value.

Kicks off jobs to run on a Dataproc cluster. The job code lives in
[jobs/ltv_daily.py](https://github.com/mozilla/telemetry-airflow/blob/main/jobs/ltv_daily.py).

See [client_ltv docs on DTMO](https://docs.telemetry.mozilla.org/datasets/search/client_ltv/reference.html).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.dataproc import (
    copy_artifacts_dev,
    get_dataproc_parameters,
    moz_dataproc_pyspark_runner,
)
from utils.gcp import bigquery_etl_query
from utils.tags import Tag

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 3, 15),
    "email": [
        "telemetry-alerts@mozilla.com",
        "akomar@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

dag = DAG(
    "ltv_daily",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=__doc__,
    tags=tags,
)

params = get_dataproc_parameters("google_cloud_airflow_dataproc")

subdag_args = default_args.copy()
subdag_args["retries"] = 0

task_id = "ltv_daily"
project = params.project_id if params.is_dev else "moz-fx-data-shared-prod"
ltv_daily = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="ltv-daily",
        cluster_name="ltv-daily-{{ ds_nodash }}",
        idle_delete_ttl=600,
        num_workers=30,
        worker_machine_type="n2-standard-16",
        optional_components=["ANACONDA"],
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        },
        additional_metadata={"PIP_PACKAGES": "lifetimes==0.11.1"},
        python_driver_code=f"gs://{params.artifact_bucket}/jobs/ltv_daily.py",
        py_args=[
            "--submission-date",
            "{{ ds }}",
            "--prediction-days",
            "364",
            "--project-id",
            project,
            "--source-qualified-table-id",
            f"{project}.search.search_rfm",
            "--dataset-id",
            "analysis",
            "--intermediate-table-id",
            "ltv_daily_temporary_search_rfm_day",
            "--model-input-table-id",
            "ltv_daily_model_perf",
            "--model-output-table-id",
            "ltv_daily",
            "--temporary-gcs-bucket",
            params.storage_bucket,
        ],
        gcp_conn_id=params.conn_id,
        service_account=params.client_email,
        artifact_bucket=params.artifact_bucket,
        storage_bucket=params.storage_bucket,
        default_args=subdag_args,
    ),
)

if params.is_dev:
    copy_to_dev = copy_artifacts_dev(
        dag, params.project_id, params.artifact_bucket, params.storage_bucket
    )
    copy_to_dev >> ltv_daily
else:
    wait_for_search_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_search_clients_last_seen",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_last_seen__v1",
        execution_delta=timedelta(hours=1),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )
    wait_for_search_clients_last_seen >> ltv_daily

ltv_revenue_join = bigquery_etl_query(
    task_id="ltv_revenue_join",
    destination_table="client_ltv_v1",
    dataset_id="revenue_derived",
    project_id="moz-fx-data-shared-prod",
    arguments=(
        "--clustering_fields=engine,country",
        "--schema_update_option=ALLOW_FIELD_ADDITION",
        "--schema_update_option=ALLOW_FIELD_RELAXATION",
        "--time_partitioning_type=DAY",
        "--time_partitioning_field=submission_date",
    ),
    dag=dag,
)

ltv_daily >> ltv_revenue_join
