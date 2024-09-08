import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker

from utils.gcp import bigquery_dq_check, bigquery_etl_query

docs = """
### ga4_site_metrics_summary_backfill

Backfills the past three days of data for moz-fx-data-shared-prod.mozilla_org_derived.www_site_metrics_summary_v2 since late data can arrive for a few days

Built from bigquery-etl repo, [`dags/bqetl_google_analytics_derived_ga4.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_google_analytics_derived_ga4.py).

This file is meant to look very similar to generated DAGs in bigquery-etl.

#### Owner

kwindau@mozilla.com
"""

default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 4, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "ga4_site_metrics_summary_backfill",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    for day_offset in ["-3", "-2", "-1"]:
        task_id = "mozilla_org_derived__www_site_metrics_summary__v2__backfill_" + day_offset
        date_str = "macros.ds_add(ds, " + day_offset + ")"
        date_str_no_dash = "macros.ds_format(" + date_str + ", '%Y-%m-%d', '%Y%m%d')"

        ga4_www_site_metrics_summary_v2_checks = bigquery_dq_check(
            task_id="checks__fail_" + task_id,
            source_table="www_site_metrics_summary_v2",
            dataset_id="mozilla_org_derived",
            project_id="moz-fx-data-shared-prod",
            is_dq_check_fail=True,
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            depends_on_past=False,
            parameters=["submission_date:DATE:{{ " + date_str + " }}"],
            retries=0,
        )

        ga4_www_site_metrics_summary_v2 = bigquery_etl_query(
            task_id=task_id,
            destination_table="www_site_metrics_summary_v2${{ "
            + date_str_no_dash
            + " }}",
            dataset_id="mozilla_org_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            parameters=["submission_date:DATE:{{ " + date_str + " }}"],
            depends_on_past=False,
        )

        todays_ga4_www_site_metrics_summary_v2 = ExternalTaskMarker(
            task_id="rerun__mozilla_org_derived__www_site_metrics_summary__v2__" + day_offset,
            external_dag_id="bqetl_google_analytics_derived_ga4",
            external_task_id="wait_for_" + task_id,
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        (
            ga4_www_site_metrics_summary_v2
            >> ga4_www_site_metrics_summary_v2_checks
            >> todays_ga4_www_site_metrics_summary_v2
        )
