from datetime import datetime, timedelta

from airflow import DAG
from timetable import MultiWeekTimetable

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### shredder

#### Description

These jobs normally need to be restarted many times because of transient
Airflow or Kubernetes API errors or query failures since each query is only
attempted once per task attempt. `main_v5` in particular has partitions
that fail often due to a combination of size, schema, and clustering.  In most cases
failed jobs may simply be restarted.

Logs from failed runs are sometimes not available in airflow because Kubernetes Pods are deleted
on exit. Instead, logs can be found in Google Cloud Logging
(change resource.labels.pod_name to get logs for different tasks):
- [shredder-telemetry-main](https://cloudlogging.app.goo.gl/irkg8mKzEy7kBqqg7)
- [shredder-all](https://cloudlogging.app.goo.gl/UVf3T7QMe4EdGQ6h9)

Kubernetes Pods are deleted on exit to prevent multiple running instances. Multiple
running instances will submit redundant queries, because state is only read at the start
of each run. This may cause queries to timeout because only a few may be run in parallel
while the rest are queued.

#### Owner

akomar@mozilla.com
"""

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2023, 5, 16),
    "catchup": False,
    "email": [
        "telemetry-alerts@mozilla.com",
        "akomar@mozilla.com",
        "bewu@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 44,
    "retry_delay": timedelta(minutes=5),
}

tags = [
    Tag.ImpactTier.tier_2,
    Tag.Triage.no_triage,
]

dag = DAG(
    "shredder",
    default_args=default_args,
    # 4 week intervals from start_date. This is similar to
    # schedule_interval=timedelta(days=28), except it should actually work.
    schedule=MultiWeekTimetable(num_weeks=4),
    doc_md=docs,
    tags=tags,
)
docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"
base_command = [
    "script/shredder_delete",
    "--state-table=moz-fx-data-shredder.shredder_state.shredder_state",
    "--task-table=moz-fx-data-shredder.shredder_state.tasks",
    # dags run one schedule interval after ds, end date should be one day before the dag
    # runs, and schedule intervals are 4 weeks = 28 days, so 28-1 = 27 days after ds
    "--end-date={{macros.ds_add(ds, 27)}}",
    # start date should be two schedule intervals before end date, to avoid
    # race conditions with downstream tables and pings received shortly after a
    # deletion request. schedule intervals are 4 weeks = 28 days.
    # This is temporarily increased to 4 intervals, in order handle outstanding backlog
    "--start-date={{macros.ds_add(ds, 27-28*4)}}",
    # non-dml statements use LEFT JOIN instead of IN to filter rows, which takes about
    # half as long as of 2022-02-14, and reduces cost by using less flat rate slot time
    "--no-use-dml",
]
common_task_args = {
    "image": docker_image,
    "is_delete_operator_pod": True,
    "reattach_on_restart": True,
    "dag": dag,
}

# handle telemetry main and main use counter separately to ensure they run continuously
# and don't slow down other tables. run them in a separate project with their own slot
# reservation to ensure they can finish on time, because they use more slots than
# everything else combined
telemetry_main = GKEPodOperator(
    task_id="telemetry_main",
    name="shredder-telemetry-main",
    arguments=[
        *base_command,
        "--parallelism=2",
        "--billing-project=moz-fx-data-shredder",
        "--only=telemetry_stable.main_v5",
    ],
    **common_task_args,
)

telemetry_main_use_counter = GKEPodOperator(
    task_id="telemetry_main_use_counter",
    name="shredder-telemetry-main-use-counter",
    arguments=[
        *base_command,
        "--parallelism=2",
        "--billing-project=moz-fx-data-shredder",
        "--only=telemetry_stable.main_use_counter_v4",
    ],
    **common_task_args,
)

# everything else
flat_rate = GKEPodOperator(
    task_id="all",
    name="shredder-all",
    arguments=[
        *base_command,
        "--parallelism={{ var.value.get('shredder_all_parallelism', 3) }}",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--except",
        "telemetry_stable.main_v5",
        "telemetry_stable.main_use_counter_v4",
        "telemetry_derived.event_events_v1",
        "firefox_desktop_derived.events_stream_v1",
    ],
    # Needed to scale the highmem pool from 0 -> 1, because cluster autoscaling
    # works on pod resource requests, instead of usage
    container_resources={
        "request_memory": "13312Mi",
        "request_cpu": None,
        "limit_memory": "20480Mi",
        "limit_cpu": None,
        "limit_gpu": None,
    },
    # This job was being killed by Kubernetes for using too much memory, thus the highmem node pool
    node_selector={"nodepool": "highmem"},
    # Give additional time since we may need to scale up when running this job
    startup_timeout_seconds=360,
    **common_task_args,
)

experiments = GKEPodOperator(
    task_id="experiments",
    name="shredder-experiments",
    arguments=[
        *base_command,
        "--parallelism=6",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--environment=experiments",
    ],
    **common_task_args,
)

# NOTE: avoid using workgroup-restricted tables with sampling because the temp dataset
# is accessible to the data platform group
with_sampling = GKEPodOperator(
    task_id="with-sampling",
    name="shredder-with-sampling",
    arguments=[
        *base_command,
        "--parallelism={{ var.value.get('shredder_w_sampling_parallelism', 1) }}",
        "--sampling-parallelism={{ var.value.get('shredder_w_sampling_sampling_parallelism', 4) }}",
        "--temp-dataset=moz-fx-data-shredder.shredder_tmp",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--only",
        "telemetry_derived.event_events_v1",
        "firefox_desktop_derived.events_stream_v1",
        "--sampling-tables",
        "telemetry_derived.event_events_v1",
        "firefox_desktop_derived.events_stream_v1",
    ],
    **common_task_args,
)
