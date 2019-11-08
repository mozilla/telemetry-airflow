from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.gcp import gke_command
from utils.mozetl import mozetl_envvar
from utils.status import register_status

common = {
    "depends_on_past": True,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}


def merge_dict(this, other):
    copied = this.copy()
    copied.update(other)
    return copied


with DAG(
    "prerelease_telemetry_aggregates",
    start_date=datetime(2018, 12, 23),
    default_args=merge_dict(
        common,
        {
            "owner": "frank@mozilla.com",
            "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
        },
    ),
    schedule_interval="@daily",
) as dag:
    prerelease_telemetry_aggregate_view = MozDatabricksSubmitRunOperator(
        task_id="prerelease_telemetry_aggregate_view",
        job_name="Prerelease Telemetry Aggregate View",
        instance_count=10,
        dev_instance_count=10,
        execution_timeout=timedelta(hours=12),
        env=mozetl_envvar(
            "aggregator",
            {
                "date": "{{ ds_nodash }}",
                "channels": "nightly,aurora,beta",
                "credentials-bucket": "telemetry-spark-emr-2",
                "credentials-prefix": "aggregator_database_envvars.json",
                "num-partitions": 10 * 32,
            },
            dev_options={"credentials-prefix": "aggregator_dev_database_envvars.json"},
            other={
                "MOZETL_GIT_PATH": "https://github.com/mozilla/python_mozaggregator.git",
                "MOZETL_EXTERNAL_MODULE": "mozaggregator",
            },
        ),
        dag=dag,
    )


with DAG(
    "release_telemetry_aggregates",
    start_date=datetime(2018, 12, 17),
    default_args=merge_dict(
        common,
        {
            "owner": "frank@mozilla.com",
            "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
        },
    ),
    schedule_interval="@daily",
) as dag:
    release_telemetry_aggregate_view = MozDatabricksSubmitRunOperator(
        task_id="release_telemetry_aggregate_view",
        job_name="Release Telemetry Aggregate View",
        instance_count=40,
        execution_timeout=timedelta(hours=12),
        env=mozetl_envvar(
            "aggregator",
            {
                "date": "{{ ds_nodash }}",
                "channels": "release",
                "credentials-bucket": "telemetry-spark-emr-2",
                "credentials-prefix": "aggregator_database_envvars.json",
                "num-partitions": 40 * 32,
            },
            dev_options={"credentials-prefix": "aggregator_dev_database_envvars.json"},
            other={
                "MOZETL_GIT_PATH": "https://github.com/mozilla/python_mozaggregator.git",
                "MOZETL_EXTERNAL_MODULE": "mozaggregator",
            },
        ),
        dag=dag,
    )


with DAG(
    "mobile_aggregates",
    start_date=datetime(2019, 1, 1),
    default_args=merge_dict(
        common,
        {
            "owner": "robhudson@mozilla.com",
            "email": [
                "telemetry-alerts@mozilla.com",
                "robhudson@mozilla.com",
                "frank@mozilla.com",
            ],
        },
    ),
    schedule_interval="@daily",
) as dag:
    mobile_aggregate_view = MozDatabricksSubmitRunOperator(
        task_id="mobile_aggregate_view",
        job_name="Mobile Aggregate View",
        instance_count=5,
        execution_timeout=timedelta(hours=12),
        env=mozetl_envvar(
            "mobile",
            {
                "date": "{{ ds_nodash }}",
                "channels": "nightly",
                "output": "s3://{{ task.__class__.private_output_bucket }}/mobile_metrics_aggregates/v2",
                "num-partitions": 5 * 32,
            },
            other={
                "MOZETL_GIT_PATH": "https://github.com/mozilla/python_mozaggregator.git",
                "MOZETL_EXTERNAL_MODULE": "mozaggregator",
            },
        ),
        dag=dag,
    )

    register_status(
        mobile_aggregate_view,
        "Mobile Aggregates",
        "Aggregates of metrics sent through the mobile-events pings.",
    )


with DAG(
    "telemetry_aggregates_parquet",
    start_date=datetime(2018, 11, 28),
    default_args=merge_dict(
        common,
        {
            "owner": "robhudson@mozilla.com",
            "email": [
                "telemetry-alerts@mozilla.com",
                "robhudson@mozilla.com",
                "frank@mozilla.com",
            ],
        },
    ),
    schedule_interval="@daily",
) as dag:
    telemetry_aggregate_parquet_view = MozDatabricksSubmitRunOperator(
        task_id="telemetry_aggregate_parquet_view",
        job_name="Telemetry Aggregate Parquet View",
        instance_count=5,
        execution_timeout=timedelta(hours=12),
        env=mozetl_envvar(
            "parquet",
            {
                "date": "{{ ds_nodash }}",
                "channels": "nightly",
                "output": "s3://{{ task.__class__.private_output_bucket }}/aggregates_poc/v1",
            },
            other={
                "MOZETL_GIT_PATH": "https://github.com/mozilla/python_mozaggregator.git",
                "MOZETL_EXTERNAL_MODULE": "mozaggregator",
            },
        ),
        dag=dag,
    )
