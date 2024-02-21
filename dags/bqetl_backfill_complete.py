"""DAG for completing registered bigquery-etl backfills."""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCKER_IMAGE = "mozilla/bigquery-etl:latest"

tags = [Tag.ImpactTier.tier_3]

with DAG(
    "bqetl_backfill_complete",
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
        text=":hourglass_flowing_sand: Completing backfill of `{{ params.qualified_table_name }}`. A snapshot of the "
        "current production data will be kept as a backup.",
        slack_conn_id="slack_api",
    ).expand(channel=slack_users)

    complete_backfill = GKEPodOperator(
        task_id="complete_backfill",
        name="complete_backfill",
        cmds=["bash", "-x", "-c"],
        arguments=["script/bqetl backfill complete {{ params.qualified_table_name }}"],
        image=DOCKER_IMAGE,
    )

    notify_processing_complete = SlackAPIPostOperator.partial(
        task_id="slack_notify_swap_complete",
        username="Backfill",
        text=":white_check_mark: Backfill is complete. Production data has been updated.",
        slack_conn_id="slack_api",
    ).expand(channel=slack_users)

    notify_initate >> complete_backfill >> notify_processing_complete
