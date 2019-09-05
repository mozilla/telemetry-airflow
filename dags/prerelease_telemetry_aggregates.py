from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar

default_args = {
    "owner": "frank@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2018, 12, 23),
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "prerelease_telemetry_aggregates",
    default_args=default_args,
    schedule_interval="@daily",
)

prerelease_telemetry_aggregate_view = MozDatabricksSubmitRunOperator(
    task_id="prerelease_telemetry_aggregate_view",
    job_name="Prerelease Telemetry Aggregate View",
    instance_count=10,
    execution_timeout=timedelta(hours=12),
    env=mozetl_ennvvar(
        "aggregator",
        {
            "date": "{{ ds_nodash }}",
            "channels": "nightly,aurora,beta",
            "credentials-bucket": "telemetry-spark-emr-2",
            "credentials-prefix": "aggregator_database_envvars.json",
            "num-partitions": 10*32,
        },
        other={
            "MOZETL_GIT_PATH": "https://github.com/mozilla/python_mozaggregator.git",
            "MOZETL_EXTERNAL_MODULE": "mozaggregator",
        },
    ),
    dag=dag,
)
