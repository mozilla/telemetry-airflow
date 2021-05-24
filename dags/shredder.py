from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command

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

dag = DAG("shredder", default_args=default_args, schedule_interval=timedelta(days=28))
docker_image = "mozilla/bigquery-etl:latest"
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
    command=base_command + [
        "--parallelism=2",
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
    command=base_command + [
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
    command=base_command + [
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
