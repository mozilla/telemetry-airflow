from airflow import DAG
from airflow.utils.dates import days_ago
from operators.airbyte_operator import AirbyteTriggerSyncOperator

with DAG(
    dag_id="airbyte_github_test",
    default_args={"owner": "bwubbo@mozilla.com"},
    schedule_interval="@daily",
    start_date=days_ago(1),
) as dag:

    airflow_test = AirbyteTriggerSyncOperator(
        task_id="airbyte_github_test",
        airbyte_conn_id="airbyte_conn_test",
        connection_id="05b03d15-3928-4dbe-83f1-aa070eeebc9f",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )
