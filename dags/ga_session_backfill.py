import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker

from utils.gcp import bigquery_dq_check, bigquery_etl_query

docs = """
### ga_session_backfill

Backfills the past three days of data for ga_sessions.

Built from bigquery-etl repo, [`dags/bqetl_mozilla_org_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mozilla_org_derived.py).

This file is meant to look very similar to generated DAGs in bigquery-etl.

#### Owner

frank@mozilla.com
"""


default_args = {
    "owner": "frank@mozilla.com",
    "start_date": datetime.datetime(2023, 11, 13, 0, 0),
    "end_date": None,
    "email": ["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "ga_sessions_backfill",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    for day_offset in ["-3", "-2", "-1"]:
        task_id = "mozilla_org_derived__ga_sessions__v1__backfill_" + day_offset
        date_str = "macros.ds_add(ds, " + day_offset + ")"
        date_str_no_dash = "macros.ds_format(" + date_str + ", '%Y-%m-%d', '%Y%m%d')"

        ga_sessions_v1_checks = bigquery_dq_check(
            task_id="checks__fail_" + task_id,
            source_table="ga_sessions_v1",
            dataset_id="mozilla_org_derived",
            project_id="moz-fx-data-shared-prod",
            is_dq_check_fail=True,
            owner="frank@mozilla.com",
            email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
            depends_on_past=False,
            parameters=["session_date:DATE:{{ " + date_str + " }}"],
            retries=0,
        )

        ga_sessions_v1 = bigquery_etl_query(
            task_id=task_id,
            destination_table="ga_sessions_v1${{ " + date_str_no_dash + " }}",
            dataset_id="mozilla_org_derived",
            project_id="moz-fx-data-shared-prod",
            owner="frank@mozilla.com",
            email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            parameters=["session_date:DATE:{{ " + date_str_no_dash + " }}"],
            depends_on_past=False,
        )

        todays_ga_sessions = ExternalTaskMarker(
            task_id="rerun__mozilla_org_derived__ga_sessions_v1__" + day_offset,
            external_dag_id="bqetl_mozilla_org_derived",
            external_task_id="wait_for_" + task_id,
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ga_sessions_v1 >> ga_sessions_v1_checks >> todays_ga_sessions
