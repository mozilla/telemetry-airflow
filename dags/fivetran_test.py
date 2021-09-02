# Todo: remove
# this is just an example for testing
from airflow import DAG
from airflow.utils.dates import days_ago
from operators.backport.fivetran.operator import FivetranOperator
from operators.backport.fivetran.sensor import FivetranSensor

with DAG('iprospect',
         default_args={"owner": "example@mozilla.com"},
         description='',
         start_date=days_ago(1),
         schedule_interval="@daily") as dag:

    iprospect_sync_start = FivetranOperator(
            task_id='iprospect-task',
            fivetran_conn_id='fivetran_default',
            connector_id='{{ var.value.connector_id }}'
    )

    iprospect_sync_wait = FivetranSensor(
            task_id='iprospect-sensor',
            fivetran_conn_id='fivetran_default',
            connector_id='{{ var.value.connector_id }}',
            poke_interval=5
    )

    iprospect_sync_start >> iprospect_sync_wait
