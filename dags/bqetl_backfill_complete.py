"""DAG for completing registered bigquery-etl backfills."""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

AUTOMATION_SLACK_CHANNEL = "#dataops-alerts"
SLACK_CONNECTION_ID = "slack_airflow_bot"
DATA_PLATFORM_WG_CHANNEL_ID = "C01E8GDG80N"
SLACK_COMMON_ARGS = {
    "username": "airflow-bot",
    "slack_conn_id": SLACK_CONNECTION_ID,
    "channel": AUTOMATION_SLACK_CHANNEL,
}
DOCKER_IMAGE = "us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest"

tags = [Tag.ImpactTier.tier_3]

default_args = {
    "email": [
        "ascholtz@mozilla.com",
        "benwu@mozilla.com",
        "wichan@mozilla.com",
    ]
}

with DAG(
    "bqetl_backfill_complete",
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
            "script/bqetl backfill scheduled --status=Complete --json_path=/airflow/xcom/return.json --ignore-old-entries",
        ],
        image=DOCKER_IMAGE,
        do_xcom_push=True,
    )

    @task_group
    def complete_backfill(backfill):
        @task
        def prepare_slack_complete_message(entry):
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )
            return (
                f"{watcher_text} :hourglass_flowing_sand: Completing backfill of `{entry['qualified_table_name']}` has started - currently swapping backfill data into production. "
                f"A snapshot of the current production data will be kept as a backup for 30 days. "
                f"You will receive another notification once the completing step is done."
            )

        notify_initiate = SlackAPIPostOperator(
            task_id="slack_notify_initate",
            text=prepare_slack_complete_message(backfill),
            **SLACK_COMMON_ARGS,
        )

        @task
        def prepare_slack_failure_message(entry):
            project, dataset, table = entry["qualified_table_name"].split(".")
            backup_table_id = (
                f"{dataset}__{table}_backup_{entry['entry_date'].replace('-', '_')}"
            )
            backup_location = (
                f"{project}.backfills_staging_derived.{backup_table_id}"
            )
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )

            return (
                f"{watcher_text} :x: Backfill completion for `{entry['qualified_table_name']}` failed. "
                "Check recent <https://workflow.telemetry.mozilla.org/dags/bqetl_backfill_complete/grid|`process_backfill` task run> for logs. "
                f"To retry the backfill, delete the backup table at `{backup_location}`. "
                f"Ask in <#{DATA_PLATFORM_WG_CHANNEL_ID}> if you need help."
            )

        @task
        def prepare_pod_parameters(entry):
            return [f"script/bqetl backfill complete { entry['qualified_table_name'] }"]

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
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )

            return f"{watcher_text} :white_check_mark: Backfill is complete for `{entry['qualified_table_name']}`. Production data has been updated."

        notify_processing_complete = SlackAPIPostOperator(
            task_id="slack_notify_processing_complete",
            text=prepare_slack_processing_complete_parameters(backfill),
            **SLACK_COMMON_ARGS,
        )

        notify_initiate >> process_backfill >> notify_processing_complete

    backfill_groups = complete_backfill.expand(backfill=detect_backfills.output)
