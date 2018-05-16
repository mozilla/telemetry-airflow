from airflow import DAG
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

dag = DAG('taar_amodump', default_args=default_args, schedule_interval='@daily')

amodump = EMRSparkOperator(
    task_id="taar_amodump",
    job_name="Dump AMO JSON blobs with oldest creation date",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar("taar_amodump",
                      {"date": "{{ ds_nodash }}"},
                      {'MOZETL_SUBMISSION_METHOD': 'python'}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag
)

amowhitelist = EMRSparkOperator(
    task_id="taar_amowhitelist",
    job_name="Generate a whitelisted set of addons for TAAR",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar("taar_amowhitelist",
                      {},
                      {'MOZETL_SUBMISSION_METHOD': 'spark'}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag
)

taar_lite = EMRSparkOperator(
    task_id="taar_lite",
    job_name="Generate GUID coinstallation JSON for TAAR",
    execution_timeout=timedelta(hours=2),
    instance_count=5,
    owner="mlopatka@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar("taar_lite",
                      {"date": "{{ ds_nodash }}"},
                      {'MOZETL_SUBMISSION_METHOD': 'python'}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag
)

# Set a dependency on amodump from amowhitelist
amowhitelist.set_upstream(amodump)

# Set a dependency on amowhitelist from taar_lite
taar_lite.set_upstream(amowhitelist)
