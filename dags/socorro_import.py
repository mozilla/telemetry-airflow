from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 11),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('socorro_import', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id="socorro_import",
                      job_name="Import crash data",
                      execution_timeout=timedelta(hours=4),
                      instance_count=5,
                      owner="amiyaguchi@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
                      env={"date": "{{ ds_nodash }}"},
                      uri="https://raw.githubusercontent.com/mozilla-services/data-pipeline/master/reports/socorro_import/ImportCrashData.ipynb",
                      output_visibility="public",
                      dag=dag)
