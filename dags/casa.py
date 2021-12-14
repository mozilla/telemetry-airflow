from datetime import datetime, timedelta

from airflow import DAG
from operators.backport.fivetran.operator import FivetranOperator
from operators.backport.fivetran.sensor import FivetranSensor

DOCS = """
### fivetran_casa

#### Description 

This DAG triggers Fivetran to import data from CASA using the 
[casa connector](https://github.com/mozilla/fivetran-connectors/tree/main/connectors/casa).

#### Owner

anicholson@mozilla.com
"""

default_args = {
    "owner": "anicholson@mozilla.com",
    "email": ["anicholson@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 10),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG('fivetran_casa',
         default_args=default_args,
         doc_md=DOCS,
         schedule_interval="0 5 * * *") as dag:

    casa_sync_start = FivetranOperator(
        connector_id='{{ var.value.fivetran_casa_connector_id }}',
        task_id='casa-task',
    )

    casa_sync_wait = FivetranSensor(
        connector_id='{{ var.value.fivetran_casa_connector_id }}',
        task_id='casa-sensor',
        poke_interval=5
    )

    casa_sync_start >> casa_sync_wait
