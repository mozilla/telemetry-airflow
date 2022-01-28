from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command
from utils.tags import Tag

docs = """
### shredder

#### Description

These jobs normally need to be restarted many times, because each query is only
attempted once per run. `main_v4` and `main_summary_v4` in particular have partitions
that fail often due to a combination of size, schema, and clustering. In most cases
failed jobs may simply be restarted.

Logs from failed runs are not available in airflow, because Kubernetes Pods are deleted
on exit. Instead, logs can be found in Google Cloud Logging:
- [shredder-flat-rate-main-summary](https://cloudlogging.app.goo.gl/Tv68VKpCR9fzbJNGA)
- [shredder-flat-rate](https://cloudlogging.app.goo.gl/Uu6VRn34VY4AryGJ9)
- [on-demand](https://cloudlogging.app.goo.gl/GX1GM9hwZMENNnnq8)

Kubernetes Pods are deleted on exit to prevent multiple running instances. Multiple
running instances will submit redundant queries, because state is only read at the start
of each run. This may cause queries to timeout because only a few may be run in parallel
while the rest are queued.

#### Owner

dthorn@mozilla.com
"""

default_args = {
    "owner": "dthorn@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 4, 7),
    "email": [
        "telemetry-alerts@mozilla.com",
        "dthorn@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 14,
    "retry_delay": timedelta(minutes=5),
}

tags = [Tag.ImpactTier.tier_2]

dag = DAG(
    "shredder",
    default_args=default_args,
    schedule_interval=timedelta(days=28),
    doc_md=docs,
    tags=tags,
)
docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"
base_command = [
    "script/shredder_delete",
    "--state-table=moz-fx-data-shredder.shredder_state.shredder_state",
    "--task-table=moz-fx-data-shredder.shredder_state.tasks",
    # dags run schedule_interval after ds, and end date should be one day
    # before the dag runs, so 28-1 = 27 days after ds.
    "--end-date={{macros.ds_add(ds, 27)}}",
    # start date should be two schedule intervals before end date, to avoid
    # race conditions with downstream tables and pings received shortly after a
    # deletion request.
    "--start-date={{macros.ds_add(ds, 27-28*2)}}",
]

# main_v4 is cheaper to handle in a project without flat-rate query pricing
on_demand = gke_command(
    task_id="on_demand",
    name="shredder-on-demand",
    command=base_command
    + [
        # temporarily cover 4 intervals instead of 2, to process backlog
        # per https://bugzilla.mozilla.org/show_bug.cgi?id=1747068
        "--start-date={{macros.ds_add(ds, 27-28*4)}}",
        # avoid dml to increase parallelism
        "--no-use-dml",
        "--parallelism=4",
        "--billing-project=moz-fx-data-shredder",
        "--only=telemetry_stable.main_v4",
    ],
    docker_image=docker_image,
    is_delete_operator_pod=True,
    dag=dag,
)

# handle main_summary separately to ensure that it doesn't slow everything else
# down and also to avoid timeout errors related to queueing when running more
# than 2 DML DELETE statements at once on a single table
flat_rate_main_summary = gke_command(
    task_id="flat_rate_main_summary",
    name="shredder-flat-rate-main-summary",
    command=base_command
    + [
        "--parallelism=2",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--only=telemetry_derived.main_summary_v4",
    ],
    docker_image=docker_image,
    is_delete_operator_pod=True,
    dag=dag,
)

# everything else
flat_rate = gke_command(
    task_id="flat_rate",
    name="shredder-flat-rate",
    command=base_command
    + [
        "--parallelism=4",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--except",
        "telemetry_stable.main_v4",
        "telemetry_derived.main_summary_v4",
    ],
    docker_image=docker_image,
    is_delete_operator_pod=True,
    dag=dag,
)
