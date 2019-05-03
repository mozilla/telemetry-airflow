from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar

default_args = {
    "owner": "wbeard@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 2, 22),
    "email": [
        "telemetry-alerts@mozilla.com",
        "wbeard@mozilla.com",
        "amiyaguchi@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

# run on the 8th hour of the 1st day of the month
dag = DAG("bgbb_fit", default_args=default_args, schedule_interval="0 8 1 * *")


bgbb_fit = MozDatabricksSubmitRunOperator(
    task_id="bgbb_fit",
    job_name="Fit BGBB parameters",
    execution_timeout=timedelta(hours=2),
    instance_count=3,
    env=mozetl_envvar(
        "bgbb_fit",
        {
            "submission-date": "{{ ds_nodash }}",
            "model-win": "120",
            "start-params": "[0.387, 0.912, 0.102, 1.504]",
            "sample-ids": "[42]",
            "sample-fraction": "1.0",
            "penalizer-coef": "0.01",
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "prefix": "bgbb/params",
        },
        other={
            "MOZETL_GIT_PATH": "https://github.com/wcbeard/bgbb_airflow.git",
            "MOZETL_EXTERNAL_MODULE": "bgbb_airflow",
        },
    ),
    dag=dag
)
