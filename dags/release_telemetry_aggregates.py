from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar

default_args = {
    "owner": "frank@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2018, 12, 17),
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "release_telemetry_aggregates",
    default_args=default_args,
    schedule_interval="@daily",
)

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
        other={
            "MOZETL_GIT_PATH": "https://github.com/mozilla/python_mozaggregator.git",
            "MOZETL_EXTERNAL_MODULE": "mozaggregator",
        },
    ),
    dag=dag,
)
