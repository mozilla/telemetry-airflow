from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from utils.mozetl import mozetl_envvar

default_args = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 30),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('landfill', default_args=default_args, schedule_interval='0 1 * * *')


landfill_sampler = MozDatabricksSubmitRunOperator(
    task_id="landfill_sampler",
    job_name="Landfill Sampler",
    retries=0,
    execution_timeout=timedelta(hours=2),
    instance_count=3,
    iam_role="arn:aws:iam::144996185633:instance-profile/databricks-ec2-landfill",
    env=mozetl_envvar("landfill_sampler", {
        "submission-date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "sanitized-landfill-sample",
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)
