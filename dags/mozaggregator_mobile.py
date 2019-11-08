from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar
from utils.status import register_status

default_args = {
    "owner": "robhudson@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2019, 1, 1),
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

dag = DAG("mobile_aggregates", default_args=default_args, schedule_interval="@daily")

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
            "num-partitions": 5*32
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
