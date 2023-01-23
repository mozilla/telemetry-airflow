from datetime import datetime, timedelta

from airflow import DAG
from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from utils.callbacks import retry_tasks_callback
from utils.tags import Tag

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

tags = [Tag.ImpactTier.tier_3]

with DAG(
    'fivetran_bugzilla',
    default_args=default_args,
    doc_md=docs,
    schedule_interval="0 5 * * *",
    tags=tags,
) as dag:

    bugzilla_sync_start = FivetranOperator(
            task_id='bugzilla-task',
            fivetran_conn_id='fivetran',
            connector_id='duel_salutation'
    )

    bugzilla_sync_wait = FivetranSensor(
            task_id='bugzilla-sensor',
            fivetran_conn_id='fivetran',
            connector_id='duel_salutation',
            poke_interval=30,
            xcom="{{ task_instance.xcom_pull('bugzilla-task') }}",
            on_retry_callback=retry_tasks_callback,
            params={'retry_tasks': ['bugzilla-task']},
    )

    bugzilla_sync_start >> bugzilla_sync_wait
