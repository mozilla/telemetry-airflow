from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### shredder-backfill

#### Description

Manually triggered DAG that handles deletion requests from a specified time period 
for a list of given tables.

`target_tables` is a list of tables formatted as `dataset.table_name` with one table per line.
The moz-fx-data-shared-prod project is assumed because shredder currently only runs 
on tables in this project.

Use the dry run parameter run shredder with the --dry-run option to validate parameters.  
Note that the shredder dry run will still dry run queries against every partition of each table
so it may take a long time to finish if a lot of tables are given.

This DAG is meant to be used to handle older deletion requests for tables that are already being
shredded.  Any provided tables that aren't already valid deletion targets will be ignored.

#### Owner

bewu@mozilla.com
"""

params = {
    "request_start_date": Param(
        default=(date.today() - timedelta(days=7)).isoformat(),
        description="First date of deletion requests to process",
        type="string",
        format="date",
    ),
    "request_end_date": Param(
        default=(date.today()).isoformat(),
        description="Last date of data (i.e. partition) to delete from",
        type="string",
        format="date",
    ),
    "target_tables": Param(
        default=["dataset.table_name"],
        description="Tables to delete from (one per line)",
        type="array",
        minItems=1,
    ),
    "dry_run": Param(default=True, type="boolean"),
}

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "catchup": False,
    "email": [
        "telemetry-alerts@mozilla.com",
        "bewu@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    # transient failures are expected and can be handled with state table
    "retries": 44,
    "retry_delay": timedelta(minutes=5),
}

tags = [
    Tag.ImpactTier.tier_3,
    Tag.Triage.no_triage,
]

NON_DRY_RUN_TASK_ID = "shredder_backfill"
DRY_RUN_TASK_ID = "shredder_backfill_dry_run"


def base_backfill_operator(dry_run):
    """Create task for backfill, filling out parameters based on dry run."""
    return GKEPodOperator(
        task_id=DRY_RUN_TASK_ID if dry_run else NON_DRY_RUN_TASK_ID,
        cmds=[
            "script/shredder_delete",
            *(["--dry-run"] if dry_run else []),
            # use different tables from scheduled task so they can be monitored separately
            "--state-table=moz-fx-data-shredder.shredder_state.shredder_state_backfill",
            "--task-table=moz-fx-data-shredder.shredder_state.tasks_backfill",
            "--end-date={{ params.request_end_date }}",
            "--start-date={{ params.request_start_date }}",
            "--no-use-dml",
            # low parallelism to reduce slot contention with scheduled task
            "--parallelism=1",
            "--billing-project=moz-fx-data-bq-batch-prod",
            "--only",
        ],
        # target_tables will be rendered as a python list
        arguments="{{ params.target_tables }}",
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        is_delete_operator_pod=True,
        reattach_on_restart=True,
    )


with DAG(
    "shredder_backfill",
    default_args=default_args,
    schedule=None,
    doc_md=docs,
    tags=tags,
    params=params,
    # needed to pass the list of tables as a list to the pod operator
    render_template_as_native_obj=True,
) as dag:
    # Use separate tasks for dry run to make logs easier to find
    dry_run_branch = BranchPythonOperator(
        task_id="dry_run_branch",
        python_callable=lambda dry_run: (
            DRY_RUN_TASK_ID if dry_run else NON_DRY_RUN_TASK_ID
        ),
        op_kwargs={"dry_run": "{{ params.dry_run }}"},
    )

    backfill_tasks = [
        base_backfill_operator(dry_run_value) for dry_run_value in (True, False)
    ]

    dry_run_branch >> backfill_tasks
