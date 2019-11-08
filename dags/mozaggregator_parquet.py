from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar

default_args = {
    "owner": "robhudson@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2018, 11, 28),
    "email": [
        "telemetry-alerts@mozilla.com",
        "robhudson@mozilla.com",
        "frank@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "telemetry_aggregates_parquet",
    default_args=default_args,
    schedule_interval="@daily",
)

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
