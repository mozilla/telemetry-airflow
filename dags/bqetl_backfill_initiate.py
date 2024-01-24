"""
This DAG detects and initiates backfills of BigQuery tables.

"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from utils.gcp import gke_command
from utils.tags import Tag

DOCKER_IMAGE = "mozilla/bigquery-etl:latest"

tags = [Tag.ImpactTier.tier_3]

with DAG(
    "bqetl_backfill_initiate",
    doc_md=__doc__,
    tags=tags,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "qualified_table_name": Param(
            default="moz-fx-data-shared-prod.dataset.table",
            description="The qualified table with the backfill entry.",
            type="string",
        ),
        "notify_emails": Param(
            default=[],
            description="Slack users to notify about backfill processing.",
            type="array",
        ),
        "entry_date": Param(
            default="2021-01-01",
            description="The entry date for the backfill being processed.",
            type="string",
        ),
    },
) as dag:

    @task
    def get_slack_users():
        context = get_current_context()
        return [
            f"@{email.split('@')[0]}" for email in context["params"]["notify_emails"]
        ]

    slack_users = get_slack_users()

    notify_initate = SlackAPIPostOperator.partial(
        task_id="slack_notify_initate",
        username="Backfill",
        text=":hourglass_flowing_sand: Initiating backfill scheduled for `{{ params.qualified_table_name }}`.",
        slack_conn_id="slack_api",
    ).expand(channel=slack_users)

    process_backfill = gke_command(
        task_id="process_backfill",
        cmds=["bash", "-x", "-c"],
        command=["script/bqetl backfill process {{ params.qualified_table_name }}"],
        docker_image=DOCKER_IMAGE,
        gcp_conn_id="google_cloud_airflow_gke",
    )

    @task
    def get_staging_location():
        # TODO: This should probably be communicated via XCom by the processing step
        context = get_current_context()
        qualified_table_name = context["params"]["qualified_table_name"]
        project, _, table = qualified_table_name.split(".")
        backfill_table_id = (
            f"{table}_{context['params']['entry_date'].replace('-', '_')}"
        )
        return f"{project}.backfills_staging_derived.{table}_{backfill_table_id}"

    notify_processing_complete = SlackAPIPostOperator.partial(
        task_id="slack_notify_processing_complete",
        username="Backfill",
        text=":white_check_mark: Backfill processing is complete. Staging location: "
        "`{{ ti.xcom_pull(task_ids='get_staging_location') }}`. "
        "Please validate that your data has changed as you expect and complete your backfill by updating the "
        "Backfill entry's status to Complete in the bigquery-etl repository.",
        slack_conn_id="slack_api",
    ).expand(channel=slack_users)

    (
        notify_initate
        >> process_backfill
        >> get_staging_location()
        >> notify_processing_complete
    )
