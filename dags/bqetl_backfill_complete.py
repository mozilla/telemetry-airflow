"""DAG for completing registered bigquery-etl backfills."""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

AUTOMATION_SLACK_CHANNEL = "#dataops-alerts"
DOCKER_IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

tags = [Tag.ImpactTier.tier_3]

with DAG(
    "bqetl_backfill_complete",
    doc_md=__doc__,
    tags=tags,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    detect_backfills = GKEPodOperator(
        task_id="detect_backfills",
        name="detect_backfills",
        cmds=["sh", "-cx"],
        arguments=[
            "script/bqetl backfill scheduled --status=Complete --json_path=/airflow/xcom/return.json"
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
            return f"{watcher_text} :hourglass_flowing_sand: Completing backfill of `{entry['qualified_table_name']}. A snapshot of the current production data will be kept as a backup."

        notify_initiate = SlackAPIPostOperator(
            task_id="slack_notify_initate",
            username="Backfill",
            slack_conn_id="slack_api",
            text=prepare_slack_complete_message(backfill),
            channel=AUTOMATION_SLACK_CHANNEL,
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
        )

        @task
        def prepare_slack_processing_complete_parameters(entry):
            watcher_text = " ".join(
                f"<@{watcher.split('@')[0]}>" for watcher in entry["watchers"]
            )

            return f"{watcher_text} :white_check_mark: Backfill is complete. Production data has been updated."

        notify_processing_complete = SlackAPIPostOperator(
            task_id="slack_notify_processing_complete",
            username="Backfill",
            slack_conn_id="slack_api",
            text=prepare_slack_processing_complete_parameters(backfill),
            channel=AUTOMATION_SLACK_CHANNEL,
        )

        notify_initiate >> process_backfill >> notify_processing_complete

    backfill_groups = complete_backfill.expand(backfill=detect_backfills.output)
