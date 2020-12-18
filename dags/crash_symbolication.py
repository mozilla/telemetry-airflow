import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator

from utils.dataproc import moz_dataproc_pyspark_runner, get_dataproc_parameters

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 11, 26),
    "email": [
        "telemetry-alerts@mozilla.com",
        "bewu@mozilla.com",
        "mcastelluccio@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=30),
}

PIP_PACKAGES = [
    "boto3==1.16.20",
    "scipy==1.5.4",
]

with DAG(
    "crash_symbolication",
    default_args=default_args,
    # dag runs daily but tasks only run on certain days
    schedule_interval="0 5 * * 0,2,4",
) as dag:
    # top_signatures_correlations uploads results to public analysis bucket
    write_aws_conn_id = "aws_dev_telemetry_public_analysis_2_rw"
    analysis_access_key, analysis_secret_key, _ = AwsHook(
        write_aws_conn_id
    ).get_credentials()

    # modules_with_missing_symbols sends results as email
    ses_aws_conn_id = "aws_data_iam_ses"
    ses_access_key, ses_secret_key, _ = AwsHook(ses_aws_conn_id).get_credentials()

    wait_for_socorro_import = ExternalTaskSensor(
        task_id="wait_for_socorro_import",
        external_dag_id="socorro_import",
        external_task_id="bigquery_load",
        check_existence=True,
        execution_delta=datetime.timedelta(hours=5),
        dag=dag,
    )

    params = get_dataproc_parameters("google_cloud_airflow_dataproc")

    modules_with_missing_symbols = SubDagOperator(
        task_id="modules_with_missing_symbols",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5",
            dag_name="modules_with_missing_symbols",
            default_args=default_args,
            cluster_name="modules-with-missing-symbols-{{ ds }}",
            job_name="modules-with-missing-symbols",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/symbolication/modules_with_missing_symbols.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": ses_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": ses_secret_key,
            },
            py_args=[
                "--run-on-days", "0",  # run monday
                "--date", "{{ ds }}"
            ],
            idle_delete_ttl="14400",
            num_workers=2,
            worker_machine_type="n1-standard-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    top_signatures_correlations = SubDagOperator(
        task_id="top_signatures_correlations",
        dag=dag,
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            image_version="1.5",
            dag_name="top_signatures_correlations",
            default_args=default_args,
            cluster_name="top-signatures-correlations-{{ ds }}",
            job_name="top-signatures-correlations",
            python_driver_code="https://raw.githubusercontent.com/mozilla/python_mozetl/main/mozetl/symbolication/top_signatures_correlations.py",
            init_actions_uris=[
                "gs://dataproc-initialization-actions/python/pip-install.sh"
            ],
            additional_metadata={"PIP_PACKAGES": " ".join(PIP_PACKAGES)},
            additional_properties={
                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark-env:AWS_ACCESS_KEY_ID": analysis_access_key,
                "spark-env:AWS_SECRET_ACCESS_KEY": analysis_secret_key,
            },
            py_args=[
                "--run-on-days", "0", "2", "4",  # run monday, wednesday, friday
                "--date", "{{ ds }}",
            ],
            idle_delete_ttl="14400",
            num_workers=2,
            worker_machine_type="n1-standard-4",
            gcp_conn_id=params.conn_id,
            service_account=params.client_email,
            storage_bucket=params.storage_bucket,
        ),
    )

    wait_for_socorro_import >> modules_with_missing_symbols
    wait_for_socorro_import >> top_signatures_correlations
