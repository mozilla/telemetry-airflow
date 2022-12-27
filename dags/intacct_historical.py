from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from operators.backport.fivetran.operator import FivetranOperator
from operators.backport.fivetran.sensor import FivetranSensor
from utils.tags import Tag

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

tags = [Tag.ImpactTier.tier_1, "repo/telemetry-airflow", ]

list_of_connectors ={
  "moz": "decently_wouldst",
  "germany": "backslid_mumps",
  "nz": "bok_chronicler",
  "france": "inst_panel",
  "pocket": "hallucinations_tipper",
  "taiwan": "lien_menace",
  "australia": "rally_screening",
  "denmark": "disparate_crib",
  "finland": "tick_rippling",
  "sweden": "dignity_palatable",
  "spain": "prophecy_original",
  "china_vie": "sifted_ale",
  "netherlands": "slow_congestive",
  "china_wofe": "readable_circumscribed",
  "belgium": "wiser_admit",
  "canada": "longevity_capturing",
  "uk": "toy_tribute"
}

with DAG(
    'fivetran_intacct_historical',
    default_args=default_args,
    doc_md=docs,
    schedule_interval="0 2 * * *",
    tags=tags,
) as dag:

    fivetran_sensors_complete = EmptyOperator(
        task_id='intacct-fivetran-sensors-complete',
    )

    for index, (location, connector_id) in enumerate(list_of_connectors.items()):

        fivetran_sync = EmptyOperator(task_id=f'intacct-{location}')

        # In order to avoid hitting DAG concurrency limits by sensor tasks below,
        # sync tasks here have variable priority weights
        fivetran_sync_start = FivetranOperator(
            task_id=f'intacct-task-{location}',
            fivetran_conn_id='fivetran',
            connector_id=connector_id,
            priority_weight=(index * 2) + 1,
        )
        fivetran_sync >> fivetran_sync_start

        # It's best if the sensor starts before the Fivetran sync is triggered to avoid any
        # chance of it missing the Fivetran sync happening, so we give it a higher priority and
        # don't set it as downstream of the sync start operator.
        fivetran_sync_wait = FivetranSensor(
            task_id=f'intacct-sensor-{location}',
            fivetran_conn_id='fivetran',
            connector_id=connector_id,
            poke_interval=30,
            execution_timeout=timedelta(hours=6),
            retries=0,
            priority_weight=fivetran_sync_start.priority_weight + 1,
        )
        fivetran_sync >> fivetran_sync_wait >> fivetran_sensors_complete
