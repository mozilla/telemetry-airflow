from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'harterrt@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 4, 24),
    'email': ['telemetry-alerts@mozilla.com', 'harterrt@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('txp_pulse', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="txp_pulse",
                      job_name="Pulse Testpilot Experiment ETL",
                      execution_timeout=timedelta(hours=2),
                      instance_count=5,
                      env={},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/txp_pulse.sh",
                      dag=dag)
