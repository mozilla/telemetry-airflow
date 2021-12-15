from datetime import datetime, timedelta

from airflow import DAG
from operators.backport.fivetran.operator import FivetranOperator
from operators.backport.fivetran.sensor import FivetranSensor
from airflow.operators.dummy import DummyOperator

docs = """
### fivetran_intacct

#### Description

This DAG triggers Fivetran to import data from Sage Intacct connector.

#### Owner

spatil@mozilla.com
"""

default_args = {
    "owner": "spatil@mozilla.com",
    "email": ["spatil@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 9),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}
list_of_connectors ={
  "moz": "depart_street",
  "germany": "tarry_poll",
  "nz": "station_carpet",
  "france": "philosophic_fancies",
  "pocket": "migrate_boneless",
  "taiwan": "mild_washhouse",
  "australia": "venture_patience",
  "denmark": "browsing_noteworthy",
  "finland": "rhythm_calculus",
  "sweden": "moralist_erupt",
  "spain": "conjugal_gestate",
  "china_vie": "preseason_emergence",
  "netherlands": "interpreting_prophecies",
  "china_wofe": "boulder_reseller",
  "belgium": "investigate_bringing",
  "canada": "mothball_brightness",
  "uk": "viscous_feasted"
}

with DAG('fivetran_intacct_historical',
         default_args=default_args,
         doc_md=docs,
         schedule_interval="0 5 * * *") as dag:

    fivetran_sensors_complete = DummyOperator(
        task_id='end',
    )

    for x,y in list_of_connectors.items():

        fivetran_sync_start = FivetranOperator(
                task_id='intacct-task-{}'.format(x),
                fivetran_conn_id='fivetran',
                connector_id='{}'.format(y)
        )

        fivetran_sync_wait = FivetranSensor(
                task_id='intacct-sensor-{}'.format(x),
                fivetran_conn_id='fivetran',
                connector_id='{}'.format(y),
                poke_interval=5
        )

        fivetran_sync_start >> fivetran_sync_wait >> fivetran_sensors_complete


