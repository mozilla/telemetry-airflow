from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from .operators.emr_spark_operator import EMRSparkOperator

from airflow.operators.dataset_status import DatasetStatusOperator
from .operators.sleep_operator import SleepOperator

default_args = {
    'owner': 'example@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2099, 5, 31),
    'email': ['example@mozilla.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG('example', default_args=default_args, schedule_interval='@daily')

spark = EMRSparkOperator(
    task_id = "spark",
    job_name = "Spark Example Job",
    instance_count = 1,
    execution_timeout=timedelta(hours=4),
    env = {"date": "{{ ds_nodash }}"},
    uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/examples/spark/example_date.ipynb",
    dag = dag)

bash = EMRSparkOperator(
    task_id = "bash",
    job_name = "Bash Example Job",
    instance_count = 1,
    execution_timeout=timedelta(hours=4),
    env = {"date": "{{ ds_nodash }}"},
    uri = "https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/examples/spark/example_date.sh",
    dag = dag)



statuspage_dag = DAG('example_statuspage', default_args=default_args)

set_outage = DatasetStatusOperator(
    task_id="outage",
    name="Test Airflow Integration",
    description="Testing an outage",
    status = "partial_outage",
    dag = statuspage_dag
)

sleep = SleepOperator(task_id="sleep", dag=statuspage_dag)

set_operational = DatasetStatusOperator(
    task_id="operational",
    name="Test Airflow Integration",
    description="Testing an outage",
    status = "operational",
    dag = statuspage_dag
)

set_outage >> sleep >> set_operational
