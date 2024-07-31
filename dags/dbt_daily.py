from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

DOCS = """\
# DBT Daily

This triggers jobs configured in dbt Cloud to run daily scheduled models that depend
on other Airflow jobs.

*Triage notes*

DBT accounts are limited at the moment, so it might not be possible to get more visibility
into failing jobs at the moment.
"""

default_args = {
    "owner": "ascholtz@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 31),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]


with DAG(
    "dbt_daily",
    doc_md=DOCS,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="0 4 * * 0",
    tags=tags,
) as dag:
    wait_for_copy_deduplicate = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    # runs dbt jobs tagged with "refresh_daily" and "scheduled_in_airflow"
    trigger_dbt_daily_cloud_run_job = DbtCloudRunJobOperator(
        task_id="trigger_dbt_daily_cloud_run_job",
        job_id=684764,
        check_interval=10,
        timeout=300,
    )

    wait_for_copy_deduplicate >> trigger_dbt_daily_cloud_run_job
