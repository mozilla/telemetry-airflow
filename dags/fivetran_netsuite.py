"""
Triggers Fivetran syncs of NetSuite transactions data to BigQuery, and creates daily backups of that NetSuite data in the EU.

See https://mozilla-hub.atlassian.net/browse/DENG-8468.

In case of failures, only the most recent DAG run should be retried.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.utils.task_group import TaskGroup
from fivetran_provider_async.operators import FivetranOperator

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "srose@mozilla.com",
    "email": [
        "integrationsnetsuite@mozilla.com",
        "srose@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "retry_delay": timedelta(minutes=30),
    "retries": 2,
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    "fivetran_netsuite",
    schedule_interval="30 0 * * *",
    start_date=datetime(2026, 3, 16, 0, 30),
    catchup=False,
    doc_md=__doc__,
    tags=[
        Tag.ImpactTier.tier_2,
        Tag.Triage.confidential,
    ],
    default_args=default_args,
) as dag:
    fivetran_netsuite_sync = FivetranOperator(
        task_id="fivetran_netsuite_sync",
        connector_id="{{ var.value.fivetran_netsuite_connector_id }}",
        deferrable=False,
        task_concurrency=1,
    )
    with TaskGroup(
        "fivetran_netsuite_sync_external"
    ) as fivetran_netsuite_sync_external:
        ExternalTaskMarker(
            task_id="private_bqetl_historical_transactions__wait_for_fivetran_netsuite_sync",
            external_dag_id="private_bqetl_historical_transactions",
            external_task_id="wait_for_fivetran_netsuite_sync",
            execution_date="{{ logical_date.replace(hour=4, minute=0).isoformat() }}",
        )
        fivetran_netsuite_sync >> fivetran_netsuite_sync_external

    netsuite_eu_backup_tables = [
        "account",
        "accountingbook",
        "accountingperiod",
        "accounttype",
        "approvalstatus",
        "classification",
        "consolidatedexchangerate",
        "country",
        "currency",
        "currencyrate",
        "customtransactiontype",
        "department",
        "entity",
        "expensecategory",
        "location",
        "subsidiary",
        "transaction",
        "transactionline",
        "transactionstatus",
        "vendor",
        "vendorcategory",
    ]
    netsuite_eu_backups = GKEPodOperator(
        task_id="netsuite_eu_backups",
        image="google/cloud-sdk:552.0.0",
        cmds=[
            "bash",
            "-c",
            (
                # Exit immediately if any command fails, and treat referencing unset variables as an error.
                "set -eu; "
                + 'test "$(date --utc --iso-8601)" = "{{ data_interval_end | ds }}" || ( echo "Historical backups aren\'t supported"; exit 1 ); '
                + f"for netsuite_table in {' '.join(netsuite_eu_backup_tables)}; do "
                # While --no_clobber and --force are seemingly contradictory options, --no_clobber takes precedence
                # to avoid overwriting any existing tables, and --force is necessary to avoid the command blocking
                # on a confirmation prompt about doing the cross-region copy operation in sync mode.
                + 'bq cp --no_clobber=true --force=true "moz-fx-data-bq-fivetran:netsuite.${netsuite_table}" "moz-fx-data-bq-fivetran:netsuite_eu_backups.${netsuite_table}_{{ ds_nodash }}"; '
                + "done"
            ),
        ],
        task_concurrency=1,
    )
    fivetran_netsuite_sync >> netsuite_eu_backups
