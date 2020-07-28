from datetime import datetime, timedelta
from utils.gcp import gke_command

from airflow import DAG
from airflow.macros import ds_add

default_args = {
    "owner": "bewu@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "email": [
        "bewu@mozilla.com",
    ],
}

PROJECT_ID = "moz-fx-data-marketing-prod"
DATASET_ID = "apple_app_store_exported"

apps = [
    ("989804926", "Firefox"),
    ("1489407738", "VPN"),
    ("1295998056", "WebXRViewer"),
    ("1314000270", "Lockwise"),
    ("1073435754", "Klar"),
    ("1055677337", "Focus"),
]

with DAG("app_store_analytics",
         default_args=default_args,
         max_active_runs=1,
         schedule_interval="@daily") as dag:

    tasks = []

    # App exports are scheduled sequentially to avoid hit api rate limit
    for i, (app_id, app_name) in enumerate(apps):
        commands = [
            "yarn",
            "--silent",  # silent to hide arguments from logs
            "export",
            "--username={{ var.value.app_store_connect_username }}",
            "--password={{ var.value.app_store_connect_password }}",
            f"--app-id={app_id}",
            f"--app-name={app_name}",
            f"--start-date={ds_add('{{ ds }}', -2)}",  # previous day data is incomplete
            f"--project={PROJECT_ID}",
            f"--dataset={DATASET_ID}",
        ]

        # First task will clear the day partition so that the only data in the table partition
        # is the data written by the current dag run and does not include unrecognized apps
        if i == 0:
            commands.append("--overwrite")

        app_store_analytics = gke_command(
            task_id=f"app_store_analytics_{app_name}",
            command=commands,
            docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/app-store-analytics-export:latest",
            gcp_conn_id="google_cloud_derived_datasets",
            dag=dag,
        )

        if i > 0:
            app_store_analytics.set_upstream(tasks[i - 1])

        tasks.append(app_store_analytics)
