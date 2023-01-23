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
    # non-dml statements use LEFT JOIN instead of IN to filter rows, which takes about
    # half as long as of 2022-02-14, and reduces cost by using less flat rate slot time
    "--no-use-dml",
]

# handle telemetry main separately to ensure it runs continuously and don't slow down
# other tables. run it in a separate project with its own slot reservation to ensure
# that it can finish on time, because it uses more slots than everything else combined
telemetry_main = gke_command(
    task_id="telemetry_main",
    name="shredder-telemetry-main",
    command=base_command
    + [
        "--parallelism=2",
        "--billing-project=moz-fx-data-shredder",
        "--only=telemetry_stable.main_v4",
        # handle additional 5 "month" backlog with current run
        "--start-date={{macros.ds_add(ds, 27-28*(2+5))}}",
    ],
    docker_image=docker_image,
    is_delete_operator_pod=True,
    reattach_on_restart=True,
    dag=dag,
)

# handle telemetry main summary separately to ensure it runs continuously and doesn't
# slow down other tables
telemetry_main_summary = gke_command(
    task_id="telemetry_main_summary",
    name="shredder-telemetry-main-summary",
    command=base_command
    + [
        "--parallelism=2",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--only=telemetry_derived.main_summary_v4",
    ],
    docker_image=docker_image,
    is_delete_operator_pod=True,
    reattach_on_restart=True,
    dag=dag,
)

# everything else
flat_rate = gke_command(
    task_id="all",
    name="shredder-all",
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
    reattach_on_restart=True,
    # Needed to scale the highmem pool from 0 -> 1, because cluster autoscaling
    # works on pod resource requests, instead of usage
    resources={
        "request_memory": "13312Mi",
        "request_cpu": None,
        "limit_memory": "20480Mi",
        "limit_cpu": None,
        "limit_gpu": None,
    },
    # This job was being killed by Kubernetes for using too much memory, thus the highmem node pool
    node_selectors={"nodepool": "highmem"},
    # Give additional time since we may need to scale up when running this job
    startup_timeout_seconds=360,
    dag=dag,
)
