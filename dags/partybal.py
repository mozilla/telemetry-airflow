from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": ["ascholtz@mozilla.com", "msamuel@mozilla.com",],
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 21),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG("partybal", default_args=default_args, schedule_interval="0 */3 * * *") as dag:

    # Built from repo https://github.com/mozilla/partybal
    partybal_image = "gcr.io/partybal/partybal:latest"

    partybal = GKEPodOperator(
        task_id="partybal",
        name="partybal",
        image=partybal_image,
        email=["ascholtz@mozilla.com", "msamuel@mozilla.com",],
        dag=dag,
    )

