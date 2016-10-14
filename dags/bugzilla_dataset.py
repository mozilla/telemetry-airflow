from airflow import DAG
from airflow.hooks import BaseHook
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'mdoglio@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 8, 18),
    'email': ['telemetry-alerts@mozilla.com', 'mdoglio@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('bugzilla_dataset', default_args=default_args, schedule_interval='@daily')

connection_details = BaseHook.get_connection('bugzilla_db')

env = {
    "DATABASE_USER": connection_details.login,
    "DATABASE_PASSWORD": connection_details.password,
    "DATABASE_HOST": connection_details.host,
    "DATABASE_PORT": connection_details.port,
    "DATABASE_NAME": connection_details.schema,
}

t0 = EMRSparkOperator(
    task_id="update_bugs",
    job_name="Bugzilla Dataset Update",
    execution_timeout=timedelta(hours=5),
    instance_count=1,
    env=env,
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/bugzilla_dataset.sh",
    dag=dag
)

