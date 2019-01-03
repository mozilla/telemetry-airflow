from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['hwoo@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# main_summary starts at hour 1. Takes 2 hours, configured to retry twice with 30 min delay.
dag = DAG('main_summary_on_time', default_args=default_args, schedule_interval='0 10 * * *')

def check_status(**kwargs):
    # Might have to use date - 1day
    date = kwargs['execution_date']
    print "date is " + str(date)
    task_name = 'main_summary'
    ti = TaskInstance(task_name, date)
    state = ti.current_state()
    if state != 'success':
        raise ValueError('Main summary has not successfully completed yet.')


check_main_summary_is_late = PythonOperator(
    task_id='check_main_summary_is_late',
    python_callable= check_status,
    provide_context=True,
    dag=dag
)

# later add statuspage by using register_status(check_main_summary_is_late, "Main Summary Check", "Quick check to see if main_summary task status has passed yet")
# This would mean a separate statuspage component would track if the main_summary dataset is delayed, or we could use the same main_summary component, and 
# have it just set to partial outage. This would require some refactoring though, because when main summary does eventually finish it wont set the component
# status back to healthy.  Currently we do this manually. Will get consensus first.
