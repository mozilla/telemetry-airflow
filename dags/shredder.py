from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command

default_args = {
    "owner": "dthorn@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 3, 9),
    "email": [
        "telemetry-alerts@mozilla.com",
        "dthorn@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("shredder", default_args=default_args, schedule_interval=timedelta(days=28))
docker_image = "mozilla/bigquery-etl:latest"
base_command = [
    "script/shredder_delete",
    "--state-table=moz-fx-data-shredder.shredder_state.shredder_state",
    "--start-date={{'2019-11-20' if ds == '2020-03-09' else macros.ds_add(ds, -7*4*2)}}",
    "--end-date={{ds}}",
]

# main_v4 is cheaper to handle in a project without flat-rate query pricing
on_demand = gke_command(
    task_id="on_demand",
    command=base_command + [
        "--parallelism=2",
        "--billing-project=moz-fx-data-shredder",
        "--only=telemetry_stable.main_v4",
    ],
    docker_image=docker_image,
    dag=dag,
)

# handle main_summary separately to ensure that it doesn't slow everything else
# down and also to avoid timeout errors related to queueing when running more
# than 2 DML DELETE statements at once on a single table
flat_rate_main_summary = gke_command(
    task_id="flat_rate_main_summary",
    command=base_command + [
        "--parallelism=2",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--only=telemetry_derived.main_summary_v4",
    ],
    docker_image=docker_image,
    dag=dag,
)

# everything else
flat_rate = gke_command(
    task_id="flat_rate",
    command=base_command + [
        "--parallelism=4",
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--except",
        "telemetry_stable.main_v4",
        "telemetry_derived.main_summary_v4",
    ],
    docker_image=docker_image,
    dag=dag,
)
