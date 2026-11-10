import datetime
import json
import os
import re
import sys
import time

import requests
from google.cloud import bigquery

# The projects adhoc.py reads INFORMATION_SCHEMA.JOBS_BY_PROJECT in.
JOB_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-airflow-gke-prod",
    "moz-fx-data-experiments",
    "moz-fx-data-bq-data-science",
    "moz-fx-data-marketing-prod",
    "moz-fx-data-backfill-2",
    "moz-fx-data-backfill-3",
    "moz-fx-data-backfill-4",
]

# Tables and views the pipeline queries, checked with a dry run, which
# enforces the same permissions as a query and costs nothing.
TABLES = [
    "moz-fx-data-shared-prod.stmo_external.queries_v1",
    "moz-fx-data-shared-prod.stmo_external.data_sources_v1",
    "moz-fx-data-shared-prod.stmo_external.users_v1",
    "moz-fx-data-shared-prod.stmo_external.query_results_v1",
    "moz-fx-data-shared-prod.stmo_external.visualizations_v1",
    "moz-fx-data-shared-prod.stmo.object_views_daily",
    "moz-fx-data-shared-prod.stmo.query_dashboards",
    "moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1",
    "moz-fx-data-shared-prod.monitoring_derived.column_size_v1",
    "moz-fx-data-shared-prod.glam_derived.events_stream_v1",
    "moz-fx-data-shared-prod.operational_monitoring_derived.projects_v1",
    "moz-fx-data-experiments.monitoring.experimenter_experiments_v1",
]

LOOKER_URL = "https://mozilla.cloud.looker.com"
PROBEINFO = "https://probeinfo.telemetry.mozilla.org"
# fetch_generated_sql.sh and fetch_metric_hub.sh clone these over HTTPS
REPOS = [
    "https://github.com/mozilla/bigquery-etl.git",
    "https://github.com/mozilla/metric-hub.git",
]

MARKER = "/* metric usage access check */"
# Seconds a query or API call may take before its check fails
TIMEOUT = 180

results = []


def check(name):
    """Run a check now and record its outcome: the detail it returns, or the
    first line of the error it raises. Each is printed as it finishes, with
    how long it took, so a slow step shows in the log while it runs."""

    def wrap(fn):
        print(f"... {name}", flush=True)
        start = time.monotonic()
        try:
            status, detail = "ok", fn() or ""
        except Exception as e:
            first = str(e).strip().splitlines()[0] if str(e).strip() else ""
            status, detail = "FAIL", f"{type(e).__name__}: {first[:300]}"
        seconds = time.monotonic() - start
        results.append((status, name, detail, seconds))
        print(f"{status:4} {name} ({seconds:.0f}s) {detail}", flush=True)
        return fn

    return wrap


def run(client, sql, **config):
    """The rows of a query, failing after TIMEOUT seconds."""
    job_config = bigquery.QueryJobConfig(job_timeout_ms=TIMEOUT * 1000, **config)
    job = client.query(f"{MARKER}\n{sql}", job_config=job_config, timeout=TIMEOUT)
    if config.get("dry_run"):
        return job
    return list(job.result(timeout=TIMEOUT))


def run_checks():
    try:
        client = bigquery.Client(project=os.environ.get("BILLING_PROJECT") or None)
    except Exception as e:
        results.append(("FAIL", "BigQuery client", f"{type(e).__name__}: {e}"[:300], 0))
        print(f"FAIL BigQuery client: {e}", flush=True)
        return

    @check("identity")
    def _():
        import google.auth
        from google.auth.transport.requests import Request

        creds, project = google.auth.default()
        creds.refresh(Request())
        who = getattr(creds, "service_account_email", None) or type(creds).__name__
        return f"{who}, credentials project {project}, billing {client.project}"

    @check("bigquery.jobs.create in the billing project")
    def _():
        return f"{run(client, 'SELECT 1 AS x')[0].x}"

    # Counts only, over an hour, since the query column is large. A missing
    # jobs.listAll is an access error; a count of accounts of 1 would mean
    # only this identity's own jobs are visible.
    for project in JOB_PROJECTS:

        @check(f"JOBS_BY_PROJECT {project}")
        def _(project=project):
            sql = f"""
SELECT
  COUNT(*) AS jobs,
  COUNT(DISTINCT user_email) AS accounts,
  COUNTIF(query IS NOT NULL) AS with_text,
FROM `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
"""
            r = run(client, sql)[0]
            note = " (only one account visible: jobs.list, not listAll?)" if r.accounts == 1 else ""
            return f"last hour: {r.jobs} jobs, {r.accounts} accounts, {r.with_text} with text{note}"

    # A table that requires a partition filter rejects a bare SELECT *, and
    # names the column, so the dry run is asked again with today's partition.
    for table in TABLES:

        @check(f"read {table}")
        def _(table=table):
            sql = f"SELECT * FROM `{table}`"
            try:
                job = run(client, sql, dry_run=True, use_query_cache=False)
            except Exception as e:
                column = re.search(r"filter over column\(s\) '([^']+)'", str(e))
                if not column:
                    raise
                sql += f" WHERE DATE({column.group(1)}) = CURRENT_DATE()"
                job = run(client, sql, dry_run=True, use_query_cache=False)
            return f"dry run ok, {job.total_bytes_processed / 1e9:,.1f} GB as checked"

    # inventory.py and lineage.py read COLUMN_FIELD_PATHS across the region,
    # which leaves out datasets it cannot see rather than failing. SCHEMATA
    # has the same visibility and answers in seconds; compare its count with
    # a run as a person.
    @check("stable datasets visible in moz-fx-data-shared-prod")
    def _():
        sql = """
SELECT COUNT(*) AS datasets
FROM `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.SCHEMATA`
WHERE ENDS_WITH(schema_name, '_stable')
"""
        return f"{run(client, sql)[0].datasets} stable datasets"

    # The pipeline writes with load jobs (write_table), CREATE TABLE AS
    # (summary.py) and CREATE VIEW (union_views). The names stay clear of
    # refs_* and artifacts_*, which union_views picks up.
    dataset = os.environ.get("WRITE_DATASET") or "mozdata.analysis"
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
    table_id = f"{dataset}.benwu_metrics_usage_access_check_{stamp}"
    view_id = f"{table_id}_view"

    @check(f"load a table into {dataset}")
    def _():
        config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("x", "INT64")],
            write_disposition="WRITE_TRUNCATE",
        )
        client.load_table_from_json([{"x": 1}], table_id, job_config=config).result(timeout=TIMEOUT)
        table = client.get_table(table_id)
        table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        client.update_table(table, ["expires"])
        return table_id

    @check(f"CREATE TABLE AS and CREATE VIEW in {dataset}")
    def _():
        run(
            client,
            f"""
CREATE OR REPLACE TABLE `{table_id}`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
AS SELECT 2 AS x""",
        )
        run(
            client,
            f"""
CREATE OR REPLACE VIEW `{view_id}`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
AS SELECT * FROM `{table_id}`""",
        )
        return view_id

    @check(f"delete from {dataset}")
    def _():
        client.delete_table(view_id, not_found_ok=True)
        client.delete_table(table_id, not_found_ok=True)
        return "deleted both"


def check_looker():
    if not (os.environ.get("LOOKERSDK_CLIENT_ID") and os.environ.get("LOOKERSDK_CLIENT_SECRET")):
        results.append(("skip", "Looker", "LOOKERSDK_CLIENT_ID and LOOKERSDK_CLIENT_SECRET not set", 0))
        return

    os.environ.setdefault("LOOKERSDK_BASE_URL", LOOKER_URL)
    os.environ.setdefault("LOOKERSDK_API_VERSION", "4.0")
    os.environ.setdefault("LOOKERSDK_VERIFY_SSL", "true")
    os.environ.setdefault("LOOKERSDK_TIMEOUT", str(TIMEOUT))

    import looker_sdk
    from looker_sdk import models40 as M

    sdk = looker_sdk.init40()
    explores = []

    @check("Looker login")
    def _():
        me = sdk.me(fields="id,display_name,role_ids")
        return f"user {me.id} ({me.display_name}), {len(me.role_ids or [])} roles"

    @check("Looker models and explores")
    def _():
        models = sdk.all_lookml_models(fields="name,project_name,explores")
        for m in models:
            if m.name == "system__activity":
                continue
            explores.extend((m.name, e.name) for e in (m.explores or []) if not e.hidden)
        return f"{len(models)} models, {len(explores)} visible explores"

    @check("Looker dashboards")
    def _():
        dashboards = sdk.all_dashboards(fields="id")
        lookml = sum("::" in str(d.id) for d in dashboards)
        return f"{len(dashboards)} dashboards, {lookml} of them LookML"

    @check("Looker System Activity history")
    def _():
        q = M.WriteQuery(
            model="system__activity",
            view="history",
            fields=["history.created_date", "history.query_run_count"],
            filters={"history.created_date": "7 days"},
            limit="10",
        )
        rows = json.loads(sdk.run_inline_query("json", q))
        return f"{len(rows)} rows for the last 7 days"

    @check("Looker generated SQL")
    def _():
        if not explores:
            raise RuntimeError("no visible explores to try")
        tried = []
        for model, explore in explores[:5]:
            fields = sdk.lookml_model_explore(model, explore, fields="fields(dimensions(name,hidden))").fields
            dims = [d.name for d in (fields.dimensions or []) if not d.hidden]
            if not dims:
                continue
            q = M.WriteQuery(model=model, view=explore, fields=dims[:1], limit="1")
            sql = sdk.run_inline_query("sql", q)
            tried.append(f"{model}.{explore}")
            if "SELECT" in sql.upper():
                return f"{model}.{explore}: {len(sql)} characters of SQL"
        raise RuntimeError(f"no SQL back from {', '.join(tried) or 'any explore tried'}")


def check_http():
    @check("probeinfo")
    def _():
        r = requests.get(f"{PROBEINFO}/v2/glean/app-listings", timeout=60)
        r.raise_for_status()
        return f"{len(r.json())} app listings"

    for repo in REPOS:

        @check(f"clone over HTTPS {repo}")
        def _(repo=repo):
            r = requests.get(f"{repo}/info/refs?service=git-upload-pack", timeout=60)
            r.raise_for_status()
            return f"{len(r.content):,} bytes of refs"


def main():
    run_checks()
    check_looker()
    check_http()
    width = max(len(name) for _, name, _, _ in results)
    print("\nSummary")
    for status, name, detail, seconds in results:
        print(f"{status:4}  {name:{width}}  {seconds:4.0f}s  {detail}")
    failed = [name for status, name, _, _ in results if status == "FAIL"]
    print(f"\n{len(results) - len(failed)} of {len(results)} checks passed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
