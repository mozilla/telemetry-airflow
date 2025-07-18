"""DAG for initiating registered bigquery-etl backfills."""

from datetime import datetime
from typing import Tuple

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

AUTOMATION_SLACK_CHANNEL = "#dataops-alerts"
SLACK_CONNECTION_ID = "overwatch_slack"
DATA_PLATFORM_WG_CHANNEL_ID = "C01E8GDG80N"
SLACK_COMMON_ARGS = {
    "username": "Backfill",
    "slack_conn_id": SLACK_CONNECTION_ID,
    "channel": AUTOMATION_SLACK_CHANNEL,
}
DOCKER_IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

tags = [Tag.ImpactTier.tier_3]

default_args = {
    "email": [
        "ascholtz@mozilla.com",
        "benwu@mozilla.com",
        "wichan@mozilla.com",
    ]
}


def parse_table_name_from_backfill(backfill_entry: dict) -> Tuple[str, str]:
    """Return (project_id, staging_table_id) for backfill entry."""
    project, dataset, table = backfill_entry["qualified_table_name"].split(".")
    backfill_table_id = (
        f"{dataset}__{table}_{backfill_entry['entry_date'].replace('-', '_')}"
    )
    staging_location = (
        f"{project}.backfills_staging_derived.{backfill_table_id}"
    )
    return project, staging_location


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
            "script/bqetl backfill scheduled --status=Initiate --json_path=/airflow/xcom/return.json --ignore-old-entries"
        ],
        image=DOCKER_IMAGE,
        do_xcom_push=True,
    )

    @task_group
    def initiate_backfill(backfill):
        @task
        def prepare_slack_initiate_message(entry):
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )
            return f"{watcher_text} :hourglass_flowing_sand: Initiating backfill scheduled for `{entry['qualified_table_name']}`.  You will receive another notification once the backfill is done."

        notify_initiate = SlackAPIPostOperator(
            task_id="slack_notify_initate",
            text=prepare_slack_initiate_message(backfill),
            **SLACK_COMMON_ARGS,
        )

        @task
        def prepare_slack_failure_message(entry):
            project, staging_location = parse_table_name_from_backfill(entry)
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )

            return (
                f"{watcher_text} :x: Backfill for `{entry['qualified_table_name']}` failed. "
                "Check recent <https://workflow.telemetry.mozilla.org/dags/bqetl_backfill_initiate/grid|`process_backfill` task run> for logs. "
                f"To retry the backfill, delete the staging table at `{staging_location}`. "
                f"Ask in <#{DATA_PLATFORM_WG_CHANNEL_ID}> if you need help."
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
            reattach_on_restart=True,
            on_failure_callback=send_slack_notification(
                text=prepare_slack_failure_message(backfill),
                **SLACK_COMMON_ARGS,
            ),
        )

        @task
        def prepare_slack_processing_complete_parameters(entry):
            project, staging_location = parse_table_name_from_backfill(entry)
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )

            return (
                f"{watcher_text} :white_check_mark: Backfill processing is done. Staging location: `{staging_location}`. "
                "Please validate that your data has changed as you expect and complete your backfill by updating the Backfill entry's status to Complete in the bigquery-etl repository. "
                "Note that the staging table will expire in 30 days, so the backfill must be completed within 30 days."
            )

        notify_processing_complete = SlackAPIPostOperator(
            task_id="slack_notify_processing_complete",
            text=prepare_slack_processing_complete_parameters(backfill),
            **SLACK_COMMON_ARGS,
        )

        notify_initiate >> process_backfill >> notify_processing_complete

    backfill_groups = initiate_backfill.expand(backfill=detect_backfills.output)
