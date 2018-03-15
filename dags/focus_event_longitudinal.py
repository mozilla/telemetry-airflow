from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 26),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('focus_event_longitudinal', default_args=default_args, schedule_interval='@weekly')

focus_event_longitudinal = EMRSparkOperator(
    task_id="focus_event_longitudinal",
    job_name="Focus Event Longitudinal View",
    execution_timeout=timedelta(hours=6),
    instance_count=10,
    env = tbv_envvar("com.mozilla.telemetry.views.GenericLongitudinalView", {
        "to": DS_WEEKLY,
        "tablename": "telemetry_focus_event_parquet",
        "output-path": "{{ task.__class__.private_output_bucket }}/focus_event_longitudinal",
        "num-parquet-files": 30,
        "ordering-columns": "seq,created"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)
