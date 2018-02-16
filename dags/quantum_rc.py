from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 17),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('quantum_release_criteria_view', default_args=default_args, schedule_interval='@weekly')

quantum_release_criteria_view = EMRSparkOperator(
    task_id="quantum_release_criteria_view",
    job_name="Quantum Release Criteria View",
    execution_timeout=timedelta(hours=2),
    instance_count=10,
    env = tbv_envvar("com.mozilla.telemetry.views.QuantumRCView", {
        "to": DS_WEEKLY,
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)
