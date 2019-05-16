from airflow import DAG
from datetime import datetime, timedelta

from operators.emr_spark_operator import EMRSparkOperator

from utils.mozetl import mozetl_envvar

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
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
    job_name="Generate an algorithmically defined set of whitelisted addons for TAAR",
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

editorial_whitelist = EMRSparkOperator(
    task_id="taar_update_whitelist",
    job_name="Generate a JSON blob from editorial reviewed addons for TAAR",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar("taar_update_whitelist",
                      {"date": "{{ ds_nodash }}"},
                      {'MOZETL_SUBMISSION_METHOD': 'spark'}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag
)

taar_lite = EMRSparkOperator(
    task_id="taar_lite",
    job_name="Generate GUID coinstallation JSON for TAAR",
    instance_count=5,
    execution_timeout=timedelta(hours=4),
    owner="mlopatka@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar("taar_lite",
                      {"date": "{{ ds_nodash }}"},
                      {'MOZETL_SUBMISSION_METHOD': 'spark'}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag
)

# Set a dependency on amodump from amowhitelist
amowhitelist.set_upstream(amodump)

# Set a dependency on amodump for the editorial reviewed whitelist of
# addons
editorial_whitelist.set_upstream(amodump)

# Set a dependency on amowhitelist from taar_lite
taar_lite.set_upstream(amowhitelist)
