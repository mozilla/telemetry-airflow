import json
import os

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from operators.backport.bigquery_operator_1_10_2 import BigQueryOperator
from six.moves.urllib.request import urlopen
from utils.dataproc import (
    moz_dataproc_pyspark_runner,
    copy_artifacts_dev,
    get_dataproc_parameters,
)


default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 3, 15),
    "email": [
        "telemetry-alerts@mozilla.com",
        "amiyaguchi@mozilla.com",
        "bmiroglio@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("ltv_daily", default_args=default_args, schedule_interval="@daily")

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
        idle_delete_ttl="600",
        num_workers=5,
        worker_machine_type="n1-standard-8",
        optional_components=["ANACONDA"],
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        },
        additional_metadata={"PIP_PACKAGES": "lifetimes==0.11.1"},
        python_driver_code="gs://{}/jobs/ltv_daily.py".format(params.artifact_bucket),
        py_args=[
            "--submission-date",
            "{{ ds }}",
            "--prediction-days",
            "364",
            "--project-id",
            project,
            "--source-qualified-table-id",
            "{project}.search.search_rfm".format(project=project),
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
        execution_delta=timedelta(hours=-1),
        check_existence=True,
        dag=dag,
    )
    wait_for_search_clients_last_seen >> ltv_daily

response = urlopen('/'.join([
    'https://raw.githubusercontent.com/mozilla/bigquery-etl/master/sql',
    'revenue_derived', 'client_ltv_v1', 'query.sql']))

BigQueryOperator.template_fields += ('query_params',)
ltv_revenue_join=BigQueryOperator(
    task_id='ltv_revenue_join',
    sql=response.read().decode('utf-8'),
    query_params=[{"name": "submission_date", "parameterType": {"type": "DATE"}, "parameterValue": {"value": "{{ ds }}"}}],
    destination_dataset_table='moz-it-eip-revenue-users.ltv_derived.client_ltv_v1${{ ds_nodash }}',
    bigquery_conn_id='google_cloud_it_revenue',
    use_legacy_sql=False,
    default_args=default_args,
    time_partitioning={"type": "DAY", "field": "submission_date"},
)

ltv_daily >> ltv_revenue_join
