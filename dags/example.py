from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

default_args = {
    'owner': 'rvitillo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'email': ['rvitillo@mozilla.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('example', default_args=default_args, schedule_interval='0 1 * * *')

t0 = EMRSparkOperator(task_id = "spark",
                      job_name = "spark-example",
                      instance_count = 1,
                      env = {"date": "{{ yesterday_ds_nodash }}"},
                      uri = "https://raw.githubusercontent.com/vitillo/telemetry-airflow/first/examples/spark/example_date.ipynb",
                      dag = dag)
