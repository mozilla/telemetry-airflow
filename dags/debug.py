from airflow import DAG
from operators.sleep_operator import SleepOperator
from datetime import datetime, timedelta
from utils.tags import Tag

default_args = {
    'owner': 'nobody@example.com',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(3),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

tags = [Tag.ImpactTier.tier_1]

dag = DAG('debug', default_args=default_args, schedule_interval='@daily', tags=tags,)

for x in range(12):
    SleepOperator(task_id='sleep{}'.format(x), sleep_time=300, dag=dag)
