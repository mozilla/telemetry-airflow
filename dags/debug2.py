from airflow import DAG
from operators.sleep_operator import SleepOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nobody@example.com',
    'depends_on_past': False,
    'start_date': datetime(2099, 5, 31),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG('debug2', default_args=default_args, schedule_interval='@daily')

t0 = SleepOperator(task_id='sleep0', sleep_time=300, dag=dag)
t1 = SleepOperator(task_id='sleep1', sleep_time=300, dag=dag)
t2 = SleepOperator(task_id='sleep2', sleep_time=300, dag=dag)
t3 = SleepOperator(task_id='sleep3', sleep_time=300, dag=dag)
t4 = SleepOperator(task_id='sleep4', sleep_time=300, dag=dag)
t5 = SleepOperator(task_id='sleep5', sleep_time=300, dag=dag)
t6 = SleepOperator(task_id='sleep6', sleep_time=300, dag=dag)
t7 = SleepOperator(task_id='sleep7', sleep_time=300, dag=dag)
t8 = SleepOperator(task_id='sleep8', sleep_time=300, dag=dag)
t9 = SleepOperator(task_id='sleep9', sleep_time=300, dag=dag)
t10 = SleepOperator(task_id='sleep10', sleep_time=300, dag=dag)
t11 = SleepOperator(task_id='sleep11', sleep_time=300, dag=dag)
