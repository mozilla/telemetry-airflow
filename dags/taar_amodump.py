from airflow import DAG
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

from operators.emr_spark_operator import EMRSparkOperator

from utils.mozetl import mozetl_envvar

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 20),
    'email': ['telemetry-alerts@mozilla.com', 'mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

try:
    dag = DAG('taar_amodump', default_args=default_args, schedule_interval='@weekly')

    amodump = EMRSparkOperator(
        task_id="taar_amodump",
        job_name="Dump AMO JSON blobs with oldest creation date",
        execution_timeout=timedelta(hours=1),
        instance_count=1,
        owner="vng@mozilla.com",
        email=["mlopatka@mozilla.com", "vng@mozilla.com", "sbird@mozilla.com"],
        env=mozetl_envvar("taar_amodump",
                          {"path": "/tmp/amo_cache",
                           "date": "{{ ds_nodash }}"},
                          {'MOZETL_SUBMISSION_METHOD': 'python'}),
        uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/taar_amodump.sh",
        output_visibility="private",
        dag=dag
    )
except AirflowException:
    pass
