from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'rvitillo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2099, 5, 31),
    'email': ['rvitillo@mozilla.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG('example', default_args=default_args, schedule_interval='@daily')

t0 = EMRSparkOperator(task_id = "spark",
                      job_name = "Spark Example Job",
                      instance_count = 1,
                      execution_timeout=timedelta(hours=4),
                      env = {"date": "{{ ds_nodash }}"},
                      uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/examples/spark/example_date.ipynb",
                      dag = dag)

t1 = EMRSparkOperator(task_id = "bash",
                      job_name = "Bash Example Job",
                      instance_count = 1,
                      execution_timeout=timedelta(hours=4),
                      env = {"date": "{{ ds_nodash }}"},
                      uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/examples/spark/example_date.sh",
                      dag = dag)
