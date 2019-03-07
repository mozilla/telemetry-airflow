from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 7),
    'email': ['hwoo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_email_alert', default_args=default_args, schedule_interval='@daily')

t1 = BashOperator(
    task_id='send_email_on_fail',
    bash_command='thiscommandshouldfail',
    dag=dag)
