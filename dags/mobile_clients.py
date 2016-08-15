from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'mdoglio@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 8, 12),
    'email': ['telemetry-alerts@mozilla.com', 'mdoglio@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('mobile_clients', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="mobile_clients",
                      job_name="Update mobile clients",
                      execution_timeout=timedelta(hours=4),
                      instance_count=5,
                      owner="mdoglio@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "mdoglio@mozilla.com"],
                      env={"date": "{{ ds_nodash }}"},
                      uri="https://raw.githubusercontent.com/mozilla-services/data-pipeline/master/reports/mobile_clients/mobile-clients.ipynb",
                      output_visibility="public",
                      dag=dag)
