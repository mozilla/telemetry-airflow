"""DAG for initiating registered bigquery-etl backfills."""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

import os
from slack_sdk import WebClient

AUTOMATION_SLACK_CHANNEL = "#dataops-alerts"
SLACK_CONNECTION_ID = "overwatch_slack"
DOCKER_IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

tags = [Tag.ImpactTier.tier_3]

default_args = {
    "email": [
        "anicholson@mozilla.com",
        "wichan@mozilla.com",
    ]
}

with DAG(
    "bqetl_backfill_initiate",
    doc_md=__doc__,
    tags=tags,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    detect_backfills = GKEPodOperator(
        task_id="detect_backfills",
        name="detect_backfills",
        cmds=["sh", "-cx"],
        arguments=[
            "script/bqetl backfill scheduled --status=Initiate --json_path=/airflow/xcom/return.json"
        ],
        image=DOCKER_IMAGE,
        do_xcom_push=True,
    )

    @task_group
    def initiate_backfill(backfill):
        def invite_watchers_to_slack_channel(entry):
            token = os.environ["SLACK_BOT_TOKEN"]
            slack_client = WebClient(token=token)

            for watcher in entry["watchers"]:
                user_id = slack_client.users_lookupByEmail(email=watcher)

                slack_client.channels_invite(
                    channel="testing_channel_id", # is it safe to share channel_id publicly?
                    user=user_id
                )

        @task
        def prepare_slack_initiate_message(entry):
            invite_watchers_to_slack_channel(entry)

            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )
            return f"{watcher_text} :hourglass_flowing_sand: Initiating backfill scheduled for `{entry['qualified_table_name']}`."

        notify_initiate = SlackAPIPostOperator(
            task_id="slack_notify_initate",
            username="Backfill",
            slack_conn_id=SLACK_CONNECTION_ID,
            text=prepare_slack_initiate_message(backfill),
            channel=AUTOMATION_SLACK_CHANNEL,
        )

        @task
        def prepare_pod_parameters(entry):
            return [f"script/bqetl backfill initiate { entry['qualified_table_name'] }"]

        process_backfill = GKEPodOperator(
            task_id="process_backfill",
            name="process_backfill",
            cmds=["sh", "-cx"],
            arguments=prepare_pod_parameters(backfill),
            image=DOCKER_IMAGE,
        )

        @task
        def prepare_slack_processing_complete_parameters(entry):
            project, _, table = entry["qualified_table_name"].split(".")
            backfill_table_id = f"{table}_{entry['entry_date'].replace('-', '_')}"
            staging_location = (
                f"{project}.backfills_staging_derived.{backfill_table_id}"
            )
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )

            return f"{watcher_text} :white_check_mark: Backfill processing is done. Staging location: `{staging_location}` Please validate that your data has changed as you expect and complete your backfill by updating the Backfill entry's status to Complete in the bigquery-etl repository."

        notify_processing_complete = SlackAPIPostOperator(
            task_id="slack_notify_processing_complete",
            username="Backfill",
            slack_conn_id=SLACK_CONNECTION_ID,
            text=prepare_slack_processing_complete_parameters(backfill),
            channel=AUTOMATION_SLACK_CHANNEL,
        )

        notify_initiate >> process_backfill >> notify_processing_complete

    backfill_groups = initiate_backfill.expand(backfill=detect_backfills.output)
