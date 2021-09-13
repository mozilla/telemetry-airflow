from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from operators.backport.fivetran.operator import FivetranOperator
from operators.backport.fivetran.sensor import FivetranSensor

docs = """
### fivetran_bugzilla

This DAG is currently under development. Failures can be ignored during Airflow triage.

#### Description

This DAG triggers Fivetran to import data from Bugzilla.

#### Owner

ascholtz@mozilla.com
"""

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 9),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG('fivetran_bugzilla',
         default_args=default_args,
         doc_md=docs,
         schedule_interval="0 5 * * *") as dag:

    bugzilla_sync_start = FivetranOperator(
            task_id='bugzilla-task',
            fivetran_conn_id='fivetran',
            connector_id='duel_salutation'
    )

    bugzilla_sync_wait = FivetranSensor(
            task_id='bugzilla-sensor',
            fivetran_conn_id='fivetran',
            connector_id='duel_salutation',
            poke_interval=5
    )

    bugzilla_sync_start >> bugzilla_sync_wait
